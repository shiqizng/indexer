package cockroach

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/indexer/helpers"
	"github.com/algorand/indexer/idb/cockroach/internal/encoding"
	pgutil "github.com/algorand/indexer/idb/cockroach/internal/util"
	"github.com/algorand/indexer/idb/cockroach/schema"
	"github.com/algorand/indexer/idb/migration"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"

	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/cockroach/internal/types"

	sdk "github.com/algorand/go-algorand-sdk/types"
	"github.com/algorand/go-algorand/ledger/ledgercore"
	itypes "github.com/algorand/indexer/types"

	config2 "github.com/algorand/go-algorand/config"
	protocol2 "github.com/algorand/go-algorand/protocol"
)

var serializable = pgx.TxOptions{IsoLevel: pgx.Serializable} // be a real ACID database
var readonlyRepeatableRead = pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}

// IndexerDb is an idb.IndexerDB implementation
type IndexerDb struct {
	readonly bool
	log      *log.Logger

	db             *pgxpool.Pool
	migration      *migration.Migration
	accountingLock sync.Mutex
}

func Init(connection string, opts idb.IndexerDbOptions, log *log.Logger) (*IndexerDb, chan struct{}, error) {
	postgresConfig, err := pgxpool.ParseConfig(connection)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't parse config: %v", err)
	}
	conn, err := pgxpool.ConnectConfig(context.Background(), postgresConfig)
	//defer conn.Close(context.Background())
	if err != nil {
		log.Fatal("failed to connect database", err)
	}

	//var now time.Time
	//err = conn.QueryRow(ctx, "SELECT NOW()").Scan(&now)
	//if err != nil {
	//	log.Fatal("failed to execute query", err)
	//}
	//
	//fmt.Println(now)

	idb := &IndexerDb{
		readonly: false,
		log:      log,
		db:       conn,
	}
	idb.init(opts)
	ch := make(chan struct{})
	close(ch)
	return idb, ch, nil
}

func (db *IndexerDb) isSetup() (bool, error) {
	query := `SELECT 0 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'metastate'`
	row := db.db.QueryRow(context.Background(), query)

	var tmp int
	fmt.Println("issetup!!!!")
	err := row.Scan(&tmp)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("isSetup() err: %w", err)
	}
	return true, nil
}

// Returns an error object and a channel that gets closed when blocking migrations
// finish running successfully.
func (db *IndexerDb) init(opts idb.IndexerDbOptions) (chan struct{}, error) {
	setup, err := db.isSetup()
	if err != nil {
		return nil, fmt.Errorf("init() err: %w", err)
	}

	if !setup {
		// new database, run setup
		_, err = db.db.Exec(context.Background(), schema.SetupPostgresSql)
		if err != nil {
			return nil, fmt.Errorf("unable to setup postgres: %v", err)
		}

		//err = db.markMigrationsAsDone()
		//if err != nil {
		//	return nil, fmt.Errorf("unable to confirm migration: %v", err)
		//}

		ch := make(chan struct{})
		close(ch)
		return ch, nil
	}

	// see postgres_migrations.go
	return db.runAvailableMigrations(opts)
}

// Close is part of idb.IndexerDb.
func (db *IndexerDb) Close() {
	db.db.Close()
}

func (db *IndexerDb) AddBlock(block *ledgercore.ValidatedBlock) error {
	db.log.Printf("AddBlock")
	return nil
}

// LoadGenesis is part of idb.IndexerDB
func (db *IndexerDb) LoadGenesis(genesis sdk.Genesis) error {
	f := func(tx pgx.Tx) error {
		// check genesis hash
		network, err := db.getNetworkState(context.Background(), tx)
		if err == idb.ErrorNotInitialized {
			networkState := types.NetworkState{
				GenesisHash: genesis.Hash(),
			}
			err = db.setNetworkState(tx, &networkState)
			if err != nil {
				return fmt.Errorf("LoadGenesis() err: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("LoadGenesis() err: %w", err)
		} else {
			if network.GenesisHash != genesis.Hash() {
				return fmt.Errorf("LoadGenesis() genesis hash not matching")
			}
		}
		setAccountStatementName := "set_account"
		query := `INSERT INTO account (addr, microalgos, rewardsbase, account_data, rewards_total, created_at, deleted) VALUES ($1, $2, 0, $3, $4, 0, false)`
		_, err = tx.Prepare(context.Background(), setAccountStatementName, query)
		if err != nil {
			return fmt.Errorf("LoadGenesis() prepare tx err: %w", err)
		}
		defer tx.Conn().Deallocate(context.Background(), setAccountStatementName)

		// this can't be refactored since it's used in totals.AddAccount
		// TODO: this line will be removed when removing accountTotals
		proto, ok := config2.Consensus[protocol2.ConsensusVersion(genesis.Proto)]
		if !ok {
			return fmt.Errorf("LoadGenesis() consensus version %s not found", genesis.Proto)
		}
		// TODO: remove accountTotals
		var ot basics.OverflowTracker
		var totals ledgercore.AccountTotals
		for ai, alloc := range genesis.Allocation {
			addr, err := sdk.DecodeAddress(alloc.Address)
			if err != nil {
				return fmt.Errorf("LoadGenesis() decode address err: %w", err)
			}
			accountData := ledgercore.ToAccountData(helpers.ConvertAccountType(alloc.State))
			_, err = tx.Exec(
				context.Background(), setAccountStatementName,
				addr[:], alloc.State.MicroAlgos,
				encoding.EncodeTrimmedLcAccountData(encoding.TrimLcAccountData(accountData)), 0)
			if err != nil {
				return fmt.Errorf("LoadGenesis() error setting genesis account[%d], %w", ai, err)
			}

			totals.AddAccount(proto, accountData, &ot)
		}

		err = db.setMetastate(
			tx, schema.AccountTotals, string(encoding.EncodeAccountTotals(&totals)))
		if err != nil {
			return fmt.Errorf("LoadGenesis() err: %w", err)
		}

		importstate := types.ImportState{
			NextRoundToAccount: 0,
		}
		err = db.setImportState(tx, &importstate)
		if err != nil {
			return fmt.Errorf("LoadGenesis() err: %w", err)
		}

		return nil
	}
	err := db.txWithRetry(serializable, f)
	if err != nil {
		return fmt.Errorf("LoadGenesis() err: %w", err)
	}

	return nil
}

// GetNextRoundToLoad is part of idb.IndexerDB
func (db *IndexerDb) GetNextRoundToAccount() (uint64, error) {
	return db.getNextRoundToAccount(context.Background(), nil)
}

// GetSpecialAccounts is part of idb.IndexerDb
func (db *IndexerDb) GetSpecialAccounts(ctx context.Context) (itypes.SpecialAddresses, error) {
	return itypes.SpecialAddresses{}, nil
}

// GetBlock is part of idb.IndexerDB
func (db *IndexerDb) GetBlock(ctx context.Context, round uint64, options idb.GetBlockOptions) (blockHeader sdk.BlockHeader, transactions []idb.TxnRow, err error) {
	tx, err := db.db.BeginTx(ctx, readonlyRepeatableRead)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx, `SELECT header FROM block_header WHERE round = $1`, round)
	var blockheaderjson []byte
	err = row.Scan(&blockheaderjson)
	if err == pgx.ErrNoRows {
		err = idb.ErrorBlockNotFound
		return
	}
	if err != nil {
		return
	}
	blockHeader, err = encoding.DecodeBlockHeader(blockheaderjson)
	if err != nil {
		return
	}

	if options.Transactions {
		out := make(chan idb.TxnRow, 1)
		query, whereArgs, err := buildTransactionQuery(idb.TransactionFilter{Round: &round, Limit: options.MaxTransactionsLimit + 1})
		if err != nil {
			err = fmt.Errorf("txn query err %v", err)
			out <- idb.TxnRow{Error: err}
			close(out)
			return sdk.BlockHeader{}, nil, err
		}

		rows, err := tx.Query(ctx, query, whereArgs...)
		if err != nil {
			err = fmt.Errorf("txn query %#v err %v", query, err)
			return sdk.BlockHeader{}, nil, err
		}

		// Unlike other spots, because we don't return a channel, we don't need
		// to worry about performing a rollback before closing the channel
		go func() {
			db.yieldTxnsThreadSimple(rows, out, nil, nil)
			close(out)
		}()

		results := make([]idb.TxnRow, 0)
		for txrow := range out {
			results = append(results, txrow)
		}
		if uint64(len(results)) > options.MaxTransactionsLimit {
			return sdk.BlockHeader{}, nil, idb.MaxTransactionsError{}
		}
		transactions = results
	}

	return blockHeader, transactions, nil
}

func (db *IndexerDb) yieldTxnsThreadSimple(rows pgx.Rows, results chan<- idb.TxnRow, countp *int, errp *error) {
	defer rows.Close()

	count := 0
	for rows.Next() {
		var round uint64
		var asset uint64
		var intra int
		var txn []byte
		var roottxn []byte
		var extra []byte
		var roundtime time.Time
		err := rows.Scan(&round, &intra, &txn, &roottxn, &extra, &asset, &roundtime)
		var row idb.TxnRow
		if err != nil {
			row.Error = err
		} else {
			row.Round = round
			row.Intra = intra
			if roottxn != nil {
				// Inner transaction.
				row.RootTxn = new(sdk.SignedTxnWithAD)
				*row.RootTxn, err = encoding.DecodeSignedTxnWithAD(roottxn)
				if err != nil {
					err = fmt.Errorf("error decoding roottxn, err: %w", err)
					row.Error = err
				}
			} else {
				// Root transaction.
				row.Txn = new(sdk.SignedTxnWithAD)
				*row.Txn, err = encoding.DecodeSignedTxnWithAD(txn)
				if err != nil {
					err = fmt.Errorf("error decoding txn, err: %w", err)
					row.Error = err
				}
			}

			row.RoundTime = roundtime
			row.AssetID = asset
			if len(extra) > 0 {
				row.Extra, err = encoding.DecodeTxnExtra(extra)
				if err != nil {
					err = fmt.Errorf("%d:%d decode txn extra, %v", row.Round, row.Intra, err)
					row.Error = err
				}
			}
		}
		results <- row
		if row.Error != nil {
			if errp != nil {
				*errp = err
			}
			goto finish
		}
		count++
	}
	if err := rows.Err(); err != nil {
		results <- idb.TxnRow{Error: err}
		if errp != nil {
			*errp = err
		}
	}
finish:
	if countp != nil {
		*countp = count
	}
}

func buildTransactionQuery(tf idb.TransactionFilter) (query string, whereArgs []interface{}, err error) {
	// TODO? There are some combinations of tf params that will
	// yield no results and we could catch that before asking the
	// database. A hopefully rare optimization.
	const maxWhereParts = 30
	whereParts := make([]string, 0, maxWhereParts)
	whereArgs = make([]interface{}, 0, maxWhereParts)
	joinParticipation := false
	partNumber := 1
	if tf.Address != nil {
		whereParts = append(whereParts, fmt.Sprintf("p.addr = $%d", partNumber))
		whereArgs = append(whereArgs, tf.Address)
		partNumber++
		if tf.AddressRole != 0 {
			addrBase64 := encoding.Base64(tf.Address)
			roleparts := make([]string, 0, 8)
			if tf.AddressRole&idb.AddressRoleSender != 0 {
				roleparts = append(roleparts, fmt.Sprintf("t.txn -> 'txn' ->> 'snd' = $%d", partNumber))
				whereArgs = append(whereArgs, addrBase64)
				partNumber++
			}
			if tf.AddressRole&idb.AddressRoleReceiver != 0 {
				roleparts = append(roleparts, fmt.Sprintf("t.txn -> 'txn' ->> 'rcv' = $%d", partNumber))
				whereArgs = append(whereArgs, addrBase64)
				partNumber++
			}
			if tf.AddressRole&idb.AddressRoleCloseRemainderTo != 0 {
				roleparts = append(roleparts, fmt.Sprintf("t.txn -> 'txn' ->> 'close' = $%d", partNumber))
				whereArgs = append(whereArgs, addrBase64)
				partNumber++
			}
			if tf.AddressRole&idb.AddressRoleAssetSender != 0 {
				roleparts = append(roleparts, fmt.Sprintf("t.txn -> 'txn' ->> 'asnd' = $%d", partNumber))
				whereArgs = append(whereArgs, addrBase64)
				partNumber++
			}
			if tf.AddressRole&idb.AddressRoleAssetReceiver != 0 {
				roleparts = append(roleparts, fmt.Sprintf("t.txn -> 'txn' ->> 'arcv' = $%d", partNumber))
				whereArgs = append(whereArgs, addrBase64)
				partNumber++
			}
			if tf.AddressRole&idb.AddressRoleAssetCloseTo != 0 {
				roleparts = append(roleparts, fmt.Sprintf("t.txn -> 'txn' ->> 'aclose' = $%d", partNumber))
				whereArgs = append(whereArgs, addrBase64)
				partNumber++
			}
			if tf.AddressRole&idb.AddressRoleFreeze != 0 {
				roleparts = append(roleparts, fmt.Sprintf("t.txn -> 'txn' ->> 'fadd' = $%d", partNumber))
				whereArgs = append(whereArgs, addrBase64)
				partNumber++
			}
			rolepart := strings.Join(roleparts, " OR ")
			whereParts = append(whereParts, "("+rolepart+")")
		}
		joinParticipation = true
	}
	if tf.MinRound != 0 {
		whereParts = append(whereParts, fmt.Sprintf("t.round >= $%d", partNumber))
		whereArgs = append(whereArgs, tf.MinRound)
		partNumber++
	}
	if tf.MaxRound != 0 {
		whereParts = append(whereParts, fmt.Sprintf("t.round <= $%d", partNumber))
		whereArgs = append(whereArgs, tf.MaxRound)
		partNumber++
	}
	if !tf.BeforeTime.IsZero() {
		whereParts = append(whereParts, fmt.Sprintf("h.realtime < $%d", partNumber))
		whereArgs = append(whereArgs, tf.BeforeTime)
		partNumber++
	}
	if !tf.AfterTime.IsZero() {
		whereParts = append(whereParts, fmt.Sprintf("h.realtime > $%d", partNumber))
		whereArgs = append(whereArgs, tf.AfterTime)
		partNumber++
	}
	if tf.AssetID != 0 || tf.ApplicationID != 0 {
		var creatableID uint64
		if tf.AssetID != 0 {
			creatableID = tf.AssetID
			if tf.ApplicationID != 0 {
				if tf.AssetID == tf.ApplicationID {
					// this is nonsense, but I'll allow it
				} else {
					return "", nil, fmt.Errorf("cannot search both assetid and appid")
				}
			}
		} else {
			creatableID = tf.ApplicationID
		}
		whereParts = append(whereParts, fmt.Sprintf("t.asset = $%d", partNumber))
		whereArgs = append(whereArgs, creatableID)
		partNumber++
	}
	if tf.AssetAmountGT != nil {
		whereParts = append(whereParts, fmt.Sprintf("(t.txn -> 'txn' -> 'aamt')::numeric(20) > $%d", partNumber))
		whereArgs = append(whereArgs, *tf.AssetAmountGT)
		partNumber++
	}
	if tf.AssetAmountLT != nil {
		whereParts = append(whereParts, fmt.Sprintf("(t.txn -> 'txn' -> 'aamt')::numeric(20) < $%d", partNumber))
		whereArgs = append(whereArgs, *tf.AssetAmountLT)
		partNumber++
	}
	if tf.TypeEnum != 0 {
		whereParts = append(whereParts, fmt.Sprintf("t.typeenum = $%d", partNumber))
		whereArgs = append(whereArgs, tf.TypeEnum)
		partNumber++
	}
	if len(tf.Txid) != 0 {
		whereParts = append(whereParts, fmt.Sprintf("t.txid = $%d", partNumber))
		whereArgs = append(whereArgs, tf.Txid)
		partNumber++
	}
	if tf.Round != nil {
		whereParts = append(whereParts, fmt.Sprintf("t.round = $%d", partNumber))
		whereArgs = append(whereArgs, *tf.Round)
		partNumber++
	}
	if tf.Offset != nil {
		whereParts = append(whereParts, fmt.Sprintf("t.intra = $%d", partNumber))
		whereArgs = append(whereArgs, *tf.Offset)
		partNumber++
	}
	if tf.OffsetLT != nil {
		whereParts = append(whereParts, fmt.Sprintf("t.intra < $%d", partNumber))
		whereArgs = append(whereArgs, *tf.OffsetLT)
		partNumber++
	}
	if tf.OffsetGT != nil {
		whereParts = append(whereParts, fmt.Sprintf("t.intra > $%d", partNumber))
		whereArgs = append(whereArgs, *tf.OffsetGT)
		partNumber++
	}
	if len(tf.SigType) != 0 {
		whereParts = append(whereParts, fmt.Sprintf("t.txn -> $%d IS NOT NULL", partNumber))
		whereArgs = append(whereArgs, tf.SigType)
		partNumber++
	}
	if len(tf.NotePrefix) > 0 {
		whereParts = append(whereParts, fmt.Sprintf("substring(decode(t.txn -> 'txn' ->> 'note', 'base64') from 1 for %d) = $%d", len(tf.NotePrefix), partNumber))
		whereArgs = append(whereArgs, tf.NotePrefix)
		partNumber++
	}
	if tf.AlgosGT != nil {
		whereParts = append(whereParts, fmt.Sprintf("(t.txn -> 'txn' -> 'amt')::bigint > $%d", partNumber))
		whereArgs = append(whereArgs, *tf.AlgosGT)
		partNumber++
	}
	if tf.AlgosLT != nil {
		whereParts = append(whereParts, fmt.Sprintf("(t.txn -> 'txn' -> 'amt')::bigint < $%d", partNumber))
		whereArgs = append(whereArgs, *tf.AlgosLT)
		partNumber++
	}
	if tf.EffectiveAmountGT != nil {
		whereParts = append(whereParts, fmt.Sprintf("((t.txn -> 'ca')::bigint + (t.txn -> 'txn' -> 'amt')::bigint) > $%d", partNumber))
		whereArgs = append(whereArgs, *tf.EffectiveAmountGT)
		partNumber++
	}
	if tf.EffectiveAmountLT != nil {
		whereParts = append(whereParts, fmt.Sprintf("((t.txn -> 'ca')::bigint + (t.txn -> 'txn' -> 'amt')::bigint) < $%d", partNumber))
		whereArgs = append(whereArgs, *tf.EffectiveAmountLT)
		partNumber++
	}
	if tf.RekeyTo != nil && (*tf.RekeyTo) {
		whereParts = append(whereParts, "(t.txn -> 'txn' -> 'rekey') IS NOT NULL")
	}

	// If returnInnerTxnOnly flag is false, then return the root transaction
	if !tf.ReturnInnerTxnOnly {
		query = "SELECT t.round, t.intra, t.txn, root.txn, t.extra, t.asset, h.realtime FROM txn t JOIN block_header h ON t.round = h.round"
	} else {
		query = "SELECT t.round, t.intra, t.txn, NULL, t.extra, t.asset, h.realtime FROM txn t JOIN block_header h ON t.round = h.round"
	}

	if joinParticipation {
		query += " JOIN txn_participation p ON t.round = p.round AND t.intra = p.intra"
	}

	// join in the root transaction if the returnInnerTxnOnly flag is false
	if !tf.ReturnInnerTxnOnly {
		query += " LEFT OUTER JOIN txn root ON t.round = root.round AND (t.extra->>'root-intra')::int = root.intra"
	}

	if len(whereParts) > 0 {
		whereStr := strings.Join(whereParts, " AND ")
		query += " WHERE " + whereStr
	}
	if joinParticipation {
		// this should match the index on txn_particpation
		query += " ORDER BY p.addr, p.round DESC, p.intra DESC"
	} else {
		// this should explicitly match the primary key on txn (round,intra)
		query += " ORDER BY t.round, t.intra"
	}
	if tf.Limit != 0 {
		query += fmt.Sprintf(" LIMIT %d", tf.Limit)
	}
	return
}

// Transactions is part of idb.IndexerDB
func (db *IndexerDb) Transactions(ctx context.Context, tf idb.TransactionFilter) (<-chan idb.TxnRow, uint64) {
	return nil, 0
}

// GetAccounts is part of idb.IndexerDB
func (db *IndexerDb) GetAccounts(ctx context.Context, opts idb.AccountQueryOptions) (<-chan idb.AccountRow, uint64) {
	return nil, 0
}

// Assets is part of idb.IndexerDB
func (db *IndexerDb) Assets(ctx context.Context, filter idb.AssetsQuery) (<-chan idb.AssetRow, uint64) {
	return nil, 0
}

// AssetBalances is part of idb.IndexerDB
func (db *IndexerDb) AssetBalances(ctx context.Context, abq idb.AssetBalanceQuery) (<-chan idb.AssetBalanceRow, uint64) {
	return nil, 0
}

// Applications is part of idb.IndexerDB
func (db *IndexerDb) Applications(ctx context.Context, filter idb.ApplicationQuery) (<-chan idb.ApplicationRow, uint64) {
	return nil, 0
}

// AppLocalState is part of idb.IndexerDB
func (db *IndexerDb) AppLocalState(ctx context.Context, filter idb.ApplicationQuery) (<-chan idb.AppLocalStateRow, uint64) {
	return nil, 0
}

// ApplicationBoxes isn't currently implemented
func (db *IndexerDb) ApplicationBoxes(ctx context.Context, filter idb.ApplicationBoxQuery) (<-chan idb.ApplicationBoxRow, uint64) {
	panic("not implemented")
}

// Health is part of idb.IndexerDB
func (db *IndexerDb) Health(ctx context.Context) (state idb.Health, err error) {
	return idb.Health{}, nil
}

// GetNetworkState is part of idb.IndexerDB
func (db *IndexerDb) GetNetworkState() (idb.NetworkState, error) {
	state, err := db.getNetworkState(context.Background(), nil)
	if err != nil {
		return idb.NetworkState{}, fmt.Errorf("GetNetworkState() err: %w", err)
	}
	networkState := idb.NetworkState{
		GenesisHash: state.GenesisHash,
	}
	return networkState, nil
}

// SetNetworkState is part of idb.IndexerDB
func (db *IndexerDb) SetNetworkState(genesis sdk.Digest) error {
	return nil
}

// SetNetworkState is part of idb.IndexerDB
func (db *IndexerDb) DeleteTransactions(ctx context.Context, keep uint64) error {
	return nil
}

// Returns `idb.ErrorNotInitialized` if uninitialized.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getMetastate(ctx context.Context, tx pgx.Tx, key string) (string, error) {
	return pgutil.GetMetastate(ctx, db.db, tx, key)
}

// If `tx` is nil, use a normal query.
func (db *IndexerDb) setMetastate(tx pgx.Tx, key, jsonStrValue string) (err error) {
	return pgutil.SetMetastate(db.db, tx, key, jsonStrValue)
}

// Returns idb.ErrorNotInitialized if uninitialized.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getImportState(ctx context.Context, tx pgx.Tx) (types.ImportState, error) {
	importStateJSON, err := db.getMetastate(ctx, tx, schema.StateMetastateKey)
	if err == idb.ErrorNotInitialized {
		return types.ImportState{}, idb.ErrorNotInitialized
	}
	if err != nil {
		return types.ImportState{}, fmt.Errorf("unable to get import state err: %w", err)
	}

	state, err := encoding.DecodeImportState([]byte(importStateJSON))
	if err != nil {
		return types.ImportState{},
			fmt.Errorf("unable to parse import state v: \"%s\" err: %w", importStateJSON, err)
	}

	return state, nil
}

// If `tx` is nil, use a normal query.
func (db *IndexerDb) setImportState(tx pgx.Tx, state *types.ImportState) error {
	return db.setMetastate(
		tx, schema.StateMetastateKey, string(encoding.EncodeImportState(state)))
}

// Returns idb.ErrorNotInitialized if uninitialized.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getNetworkState(ctx context.Context, tx pgx.Tx) (types.NetworkState, error) {
	networkStateJSON, err := db.getMetastate(ctx, tx, schema.NetworkMetaStateKey)
	if err == idb.ErrorNotInitialized {
		return types.NetworkState{}, idb.ErrorNotInitialized
	}
	if err != nil {
		return types.NetworkState{}, fmt.Errorf("unable to get network state err: %w", err)
	}

	state, err := encoding.DecodeNetworkState([]byte(networkStateJSON))
	if err != nil {
		return types.NetworkState{},
			fmt.Errorf("unable to parse network state v: \"%s\" err: %w", networkStateJSON, err)
	}

	return state, nil
}

// If `tx` is nil, use a normal query.
func (db *IndexerDb) setNetworkState(tx pgx.Tx, state *types.NetworkState) error {
	return db.setMetastate(
		tx, schema.NetworkMetaStateKey, string(encoding.EncodeNetworkState(state)))
}

// Returns ErrorNotInitialized if genesis is not loaded.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getNextRoundToAccount(ctx context.Context, tx pgx.Tx) (uint64, error) {
	state, err := db.getImportState(ctx, tx)
	if err == idb.ErrorNotInitialized {
		return 0, err
	}
	if err != nil {
		return 0, fmt.Errorf("getNextRoundToAccount() err: %w", err)
	}

	return state.NextRoundToAccount, nil
}

func (db *IndexerDb) txWithRetry(opts pgx.TxOptions, f func(pgx.Tx) error) error {
	return pgutil.TxWithRetry(db.db, opts, f, db.log)
}
