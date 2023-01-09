package cockroach

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/algorand/go-algorand/data/basics"
	models "github.com/algorand/indexer/api/generated/v2"
	"github.com/algorand/indexer/helpers"
	"github.com/algorand/indexer/idb/cockroach/internal/encoding"
	pgutil "github.com/algorand/indexer/idb/cockroach/internal/util"
	"github.com/algorand/indexer/idb/cockroach/internal/writer"
	"github.com/algorand/indexer/idb/cockroach/schema"
	"github.com/algorand/indexer/idb/migration"
	"github.com/algorand/indexer/protocol"
	"github.com/algorand/indexer/protocol/config"
	"github.com/algorand/indexer/util"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
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

type getAccountsRequest struct {
	opts        idb.AccountQueryOptions
	blockheader sdk.BlockHeader
	query       string
	rows        pgx.Rows
	out         chan idb.AccountRow
	start       time.Time
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
	//return db.runAvailableMigrations(opts)
	ch := make(chan struct{})
	close(ch)
	return ch, nil
}

// Close is part of idb.IndexerDb.
func (db *IndexerDb) Close() {
	db.db.Close()
}

func (db *IndexerDb) AddBlock(vblk *ledgercore.ValidatedBlock) error {
	// TODO: use a converter util until vblk type is changed
	vb, err := helpers.ConvertValidatedBlock(*vblk)
	if err != nil {
		return fmt.Errorf("AddBlock() err: %w", err)
	}
	block := vb.Block
	round := block.BlockHeader.Round
	db.log.Printf("adding block %d", round)

	db.accountingLock.Lock()
	defer db.accountingLock.Unlock()

	f := func(tx pgx.Tx) error {
		// Check and increment next round counter.
		importstate, err := db.getImportState(context.Background(), tx)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}
		if round != sdk.Round(importstate.NextRoundToAccount) {
			return fmt.Errorf(
				"AddBlock() adding block round %d but next round to account is %d",
				round, importstate.NextRoundToAccount)
		}
		importstate.NextRoundToAccount++
		err = db.setImportState(tx, &importstate)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}

		w, err := writer.MakeWriter(tx)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}
		defer w.Close()

		if round == sdk.Round(0) {
			err = w.AddBlock0(&block)
			if err != nil {
				return fmt.Errorf("AddBlock() err: %w", err)
			}
			return nil
		}

		var wg sync.WaitGroup
		defer wg.Wait()

		var err0 error
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := func(tx pgx.Tx) error {
				err := writer.AddTransactions(&block, block.Payset, tx)
				if err != nil {
					return err
				}
				return writer.AddTransactionParticipation(&block, tx)
			}
			err0 = db.txWithRetry(serializable, f)
		}()

		err = w.AddBlock(&block, vb.Delta)
		if err != nil {
			return fmt.Errorf("AddBlock() err: %w", err)
		}

		// Wait for goroutines to finish and check for errors. If there is an error, we
		// return our own error so that the main transaction does not commit. Hence,
		// `txn` and `txn_participation` tables can only be ahead but not behind
		// the other state.
		wg.Wait()
		isUniqueViolationFunc := func(err error) bool {
			var pgerr *pgconn.PgError
			return errors.As(err, &pgerr) && (pgerr.Code == pgerrcode.UniqueViolation)
		}
		if (err0 != nil) && !isUniqueViolationFunc(err0) {
			return fmt.Errorf("AddBlock() err0: %w", err0)
		}

		return nil
	}
	err = db.txWithRetry(serializable, f)
	if err != nil {
		return fmt.Errorf("AddBlock() err: %w", err)
	}

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
// Transactions is part of idb.IndexerDB
func (db *IndexerDb) Transactions(ctx context.Context, tf idb.TransactionFilter) (<-chan idb.TxnRow, uint64) {
	out := make(chan idb.TxnRow, 1)

	tx, err := db.db.BeginTx(ctx, readonlyRepeatableRead)
	if err != nil {
		out <- idb.TxnRow{Error: err}
		close(out)
		return out, 0
	}

	round, err := db.getMaxRoundAccounted(ctx, tx)
	if err != nil {
		out <- idb.TxnRow{Error: err}
		close(out)
		if rerr := tx.Rollback(ctx); rerr != nil {
			db.log.Printf("rollback error: %s", rerr)
		}
		return out, round
	}

	go func() {
		db.yieldTxns(ctx, tx, tf, out)
		// Because we return a channel into a "callWithTimeout" function,
		// We need to make sure that rollback is called before close()
		// otherwise we can end up with a situation where "callWithTimeout"
		// will cancel our context, resulting in connection pool churn
		if rerr := tx.Rollback(ctx); rerr != nil {
			db.log.Printf("rollback error: %s", rerr)
		}
		close(out)
	}()

	return out, round
}

// This function blocks. `tx` must be non-nil.
func (db *IndexerDb) yieldTxns(ctx context.Context, tx pgx.Tx, tf idb.TransactionFilter, out chan<- idb.TxnRow) {
	if len(tf.NextToken) > 0 {
		db.txnsWithNext(ctx, tx, tf, out)
		return
	}

	query, whereArgs, err := buildTransactionQuery(tf)
	if err != nil {
		err = fmt.Errorf("txn query err %v", err)
		out <- idb.TxnRow{Error: err}
		return
	}

	rows, err := tx.Query(ctx, query, whereArgs...)
	if err != nil {
		err = fmt.Errorf("txn query %#v err %v", query, err)
		out <- idb.TxnRow{Error: err}
		return
	}
	db.yieldTxnsThreadSimple(rows, out, nil, nil)
}

// This function blocks. `tx` must be non-nil.
func (db *IndexerDb) txnsWithNext(ctx context.Context, tx pgx.Tx, tf idb.TransactionFilter, out chan<- idb.TxnRow) {
	// TODO: Use txid to deduplicate next resultset at the query level?

	// Check for remainder of round from previous page.
	nextround, nextintra32, err := idb.DecodeTxnRowNext(tf.NextToken)
	nextintra := uint64(nextintra32)
	if err != nil {
		out <- idb.TxnRow{Error: err}
		return
	}
	origRound := tf.Round
	origOLT := tf.OffsetLT
	origOGT := tf.OffsetGT
	if tf.Address != nil {
		// (round,intra) descending into the past
		if nextround == 0 && nextintra == 0 {
			return
		}
		tf.Round = &nextround
		tf.OffsetLT = &nextintra
	} else {
		// (round,intra) ascending into the future
		tf.Round = &nextround
		tf.OffsetGT = &nextintra
	}
	query, whereArgs, err := buildTransactionQuery(tf)
	if err != nil {
		err = fmt.Errorf("txn query err %v", err)
		out <- idb.TxnRow{Error: err}
		return
	}
	rows, err := tx.Query(ctx, query, whereArgs...)
	if err != nil {
		err = fmt.Errorf("txn query %#v err %v", query, err)
		out <- idb.TxnRow{Error: err}
		return
	}

	count := 0
	db.yieldTxnsThreadSimple(rows, out, &count, &err)
	if err != nil {
		return
	}

	// If we haven't reached the limit, restore the original filter and
	// re-run the original search with new Min/Max round and reduced limit.
	if uint64(count) >= tf.Limit {
		return
	}
	tf.Limit -= uint64(count)
	select {
	case <-ctx.Done():
		return
	default:
	}
	tf.Round = origRound
	if tf.Address != nil {
		// (round,intra) descending into the past
		tf.OffsetLT = origOLT

		if nextround <= 1 {
			// NO second query
			return
		}

		tf.MaxRound = nextround - 1
	} else {
		// (round,intra) ascending into the future
		tf.OffsetGT = origOGT
		tf.MinRound = nextround + 1
	}
	query, whereArgs, err = buildTransactionQuery(tf)
	if err != nil {
		err = fmt.Errorf("txn query err %v", err)
		out <- idb.TxnRow{Error: err}
		return
	}
	rows, err = tx.Query(ctx, query, whereArgs...)
	if err != nil {
		err = fmt.Errorf("txn query %#v err %v", query, err)
		out <- idb.TxnRow{Error: err}
		return
	}
	db.yieldTxnsThreadSimple(rows, out, nil, nil)
}

// GetAccounts is part of idb.IndexerDB
func (db *IndexerDb) GetAccounts(ctx context.Context, opts idb.AccountQueryOptions) (<-chan idb.AccountRow, uint64) {
	out := make(chan idb.AccountRow, 1)

	if opts.HasAssetID == 0 && (opts.AssetGT != nil || opts.AssetLT != nil) {
		err := fmt.Errorf("AssetGT=%d, AssetLT=%d, but HasAssetID=%d", uintOrDefault(opts.AssetGT), uintOrDefault(opts.AssetLT), opts.HasAssetID)
		out <- idb.AccountRow{Error: err}
		close(out)
		return out, 0
	}

	// Begin transaction so we get everything at one consistent point in time and round of accounting.
	tx, err := db.db.BeginTx(ctx, readonlyRepeatableRead)
	if err != nil {
		err = fmt.Errorf("account tx err %v", err)
		out <- idb.AccountRow{Error: err}
		close(out)
		return out, 0
	}

	// Get round number through which accounting has been updated
	round, err := db.getMaxRoundAccounted(ctx, tx)
	if err != nil {
		err = fmt.Errorf("account round err %v", err)
		out <- idb.AccountRow{Error: err}
		close(out)
		if rerr := tx.Rollback(ctx); rerr != nil {
			db.log.Printf("rollback error: %s", rerr)
		}
		return out, round
	}

	// Get block header for that round so we know protocol and rewards info
	row := tx.QueryRow(ctx, `SELECT header FROM block_header WHERE round = $1`, round)
	var headerjson []byte
	err = row.Scan(&headerjson)
	if err != nil {
		err = fmt.Errorf("account round header %d err %v", round, err)
		out <- idb.AccountRow{Error: err}
		close(out)
		if rerr := tx.Rollback(ctx); rerr != nil {
			db.log.Printf("rollback error: %s", rerr)
		}
		return out, round
	}
	blockheader, err := encoding.DecodeBlockHeader(headerjson)
	if err != nil {
		err = fmt.Errorf("account round header %d err %v", round, err)
		out <- idb.AccountRow{Error: err}
		close(out)
		if rerr := tx.Rollback(ctx); rerr != nil {
			db.log.Printf("rollback error: %s", rerr)
		}
		return out, round
	}

	// Enforce max combined # of app & asset resources per account limit, if set
	if opts.MaxResources != 0 {
		err = db.checkAccountResourceLimit(ctx, tx, opts)
		if err != nil {
			out <- idb.AccountRow{Error: err}
			close(out)
			if rerr := tx.Rollback(ctx); rerr != nil {
				db.log.Printf("rollback error: %s", rerr)
			}
			return out, round
		}
	}

	// Construct query for fetching accounts...
	query, whereArgs := db.buildAccountQuery(opts, false)
	req := &getAccountsRequest{
		opts:        opts,
		blockheader: blockheader,
		query:       query,
		out:         out,
		start:       time.Now(),
	}
	req.rows, err = tx.Query(ctx, query, whereArgs...)
	if err != nil {
		err = fmt.Errorf("account query %#v err %v", query, err)
		out <- idb.AccountRow{Error: err}
		close(out)
		if rerr := tx.Rollback(ctx); rerr != nil {
			db.log.Printf("rollback error: %s", rerr)
		}
		return out, round
	}
	go func() {
		db.yieldAccountsThread(req)
		// Because we return a channel into a "callWithTimeout" function,
		// We need to make sure that rollback is called before close()
		// otherwise we can end up with a situation where "callWithTimeout"
		// will cancel our context, resulting in connection pool churn
		if rerr := tx.Rollback(ctx); rerr != nil {
			db.log.Printf("rollback error: %s", rerr)
		}
		close(req.out)
	}()
	return out, round
}

var statusStrings = []string{"Offline", "Online", "NotParticipating"}

const offlineStatusIdx = 0

func tealValueToModel(tv basics.TealValue) models.TealValue {
	switch tv.Type {
	case basics.TealUintType:
		return models.TealValue{
			Uint: tv.Uint,
			Type: uint64(tv.Type),
		}
	case basics.TealBytesType:
		return models.TealValue{
			Bytes: encoding.Base64([]byte(tv.Bytes)),
			Type:  uint64(tv.Type),
		}
	}
	return models.TealValue{}
}

func tealKeyValueToModel(tkv basics.TealKeyValue) *models.TealKeyValueStore {
	if len(tkv) == 0 {
		return nil
	}
	var out models.TealKeyValueStore = make([]models.TealKeyValue, len(tkv))
	pos := 0
	for key, tv := range tkv {
		out[pos].Key = encoding.Base64([]byte(key))
		out[pos].Value = tealValueToModel(tv)
		pos++
	}
	return &out
}

func (db *IndexerDb) yieldAccountsThread(req *getAccountsRequest) {
	count := uint64(0)
	defer func() {
		req.rows.Close()

		end := time.Now()
		dt := end.Sub(req.start)
		if dt > (1 * time.Second) {
			db.log.Warnf("long query %fs: %s", dt.Seconds(), req.query)
		}
	}()
	for req.rows.Next() {
		var addr []byte
		var microalgos uint64
		var rewardstotal uint64
		var createdat sql.NullInt64
		var closedat sql.NullInt64
		var deleted sql.NullBool
		var rewardsbase uint64
		var keytype *string
		var accountDataJSONStr []byte

		// below are bytes of json serialization

		// holding* are a triplet of lists that should merge together
		var holdingAssetids []byte
		var holdingAmount []byte
		var holdingFrozen []byte
		var holdingCreatedBytes []byte
		var holdingClosedBytes []byte
		var holdingDeletedBytes []byte

		// assetParams* are a pair of lists that should merge together
		var assetParamsIds []byte
		var assetParamsStr []byte
		var assetParamsCreatedBytes []byte
		var assetParamsClosedBytes []byte
		var assetParamsDeletedBytes []byte

		// appParam* are a pair of lists that should merge together
		var appParamIndexes []byte // [appId, ...]
		var appParams []byte       // [{AppParams}, ...]
		var appCreatedBytes []byte
		var appClosedBytes []byte
		var appDeletedBytes []byte

		// localState* are a pair of lists that should merge together
		var localStateAppIds []byte // [appId, ...]
		var localStates []byte      // [{local state}, ...]
		var localStateCreatedBytes []byte
		var localStateClosedBytes []byte
		var localStateDeletedBytes []byte

		// build list of columns to scan using include options like buildAccountQuery
		cols := []interface{}{&addr, &microalgos, &rewardstotal, &createdat, &closedat, &deleted, &rewardsbase, &keytype, &accountDataJSONStr}
		if req.opts.IncludeAssetHoldings {
			cols = append(cols, &holdingAssetids, &holdingAmount, &holdingFrozen, &holdingCreatedBytes, &holdingClosedBytes, &holdingDeletedBytes)
		}
		if req.opts.IncludeAssetParams {
			cols = append(cols, &assetParamsIds, &assetParamsStr, &assetParamsCreatedBytes, &assetParamsClosedBytes, &assetParamsDeletedBytes)
		}
		if req.opts.IncludeAppParams {
			cols = append(cols, &appParamIndexes, &appParams, &appCreatedBytes, &appClosedBytes, &appDeletedBytes)
		}
		if req.opts.IncludeAppLocalState {
			cols = append(cols, &localStateAppIds, &localStates, &localStateCreatedBytes, &localStateClosedBytes, &localStateDeletedBytes)
		}

		err := req.rows.Scan(cols...)
		if err != nil {
			err = fmt.Errorf("account scan err %v", err)
			req.out <- idb.AccountRow{Error: err}
			break
		}

		var account models.Account
		var aaddr basics.Address
		copy(aaddr[:], addr)
		account.Address = aaddr.String()
		account.Round = uint64(req.blockheader.Round)
		account.AmountWithoutPendingRewards = microalgos
		account.Rewards = rewardstotal
		account.CreatedAtRound = nullableInt64Ptr(createdat)
		account.ClosedAtRound = nullableInt64Ptr(closedat)
		account.Deleted = nullableBoolPtr(deleted)
		account.RewardBase = new(uint64)
		*account.RewardBase = rewardsbase
		// default to Offline in there have been no keyreg transactions.
		account.Status = statusStrings[offlineStatusIdx]
		if keytype != nil && *keytype != "" {
			account.SigType = (*models.AccountSigType)(keytype)
		}

		{
			var accountData ledgercore.AccountData
			accountData, err = encoding.DecodeTrimmedLcAccountData(accountDataJSONStr)
			if err != nil {
				err = fmt.Errorf("account decode err (%s) %v", accountDataJSONStr, err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			account.Status = statusStrings[accountData.Status]
			hasSel := !allZero(accountData.SelectionID[:])
			hasVote := !allZero(accountData.VoteID[:])
			hasStateProofkey := !allZero(accountData.StateProofID[:])

			if hasSel || hasVote || hasStateProofkey {
				part := new(models.AccountParticipation)
				if hasSel {
					part.SelectionParticipationKey = accountData.SelectionID[:]
				}
				if hasVote {
					part.VoteParticipationKey = accountData.VoteID[:]
				}
				if hasStateProofkey {
					part.StateProofKey = byteSlicePtr(accountData.StateProofID[:])
				}
				part.VoteFirstValid = uint64(accountData.VoteFirstValid)
				part.VoteLastValid = uint64(accountData.VoteLastValid)
				part.VoteKeyDilution = accountData.VoteKeyDilution
				account.Participation = part
			}

			if !accountData.AuthAddr.IsZero() {
				var spendingkey basics.Address
				copy(spendingkey[:], accountData.AuthAddr[:])
				account.AuthAddr = stringPtr(spendingkey.String())
			}

			{
				totalSchema := models.ApplicationStateSchema{
					NumByteSlice: accountData.TotalAppSchema.NumByteSlice,
					NumUint:      accountData.TotalAppSchema.NumUint,
				}
				if totalSchema != (models.ApplicationStateSchema{}) {
					account.AppsTotalSchema = &totalSchema
				}
			}
			if accountData.TotalExtraAppPages != 0 {
				account.AppsTotalExtraPages = uint64Ptr(uint64(accountData.TotalExtraAppPages))
			}

			account.TotalAppsOptedIn = accountData.TotalAppLocalStates
			account.TotalCreatedApps = accountData.TotalAppParams
			account.TotalAssetsOptedIn = accountData.TotalAssets
			account.TotalCreatedAssets = accountData.TotalAssetParams

			account.TotalBoxes = accountData.TotalBoxes
			account.TotalBoxBytes = accountData.TotalBoxBytes
		}

		if account.Status == "NotParticipating" {
			account.PendingRewards = 0
		} else {
			// TODO: pending rewards calculation doesn't belong in database layer (this is just the most covenient place which has all the data)
			// TODO: replace config.Consensus. config.Consensus map[protocol.ConsensusVersion]ConsensusParams
			// temporarily cast req.blockheader.CurrentProtocol(string) to protocol.ConsensusVersion
			proto, ok := config.Consensus[protocol.ConsensusVersion(req.blockheader.CurrentProtocol)]
			if !ok {
				err = fmt.Errorf("get protocol err (%s)", req.blockheader.CurrentProtocol)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			rewardsUnits := uint64(0)
			if proto.RewardUnit != 0 {
				rewardsUnits = microalgos / proto.RewardUnit
			}
			rewardsDelta := req.blockheader.RewardsLevel - rewardsbase
			account.PendingRewards = rewardsUnits * rewardsDelta
		}
		account.Amount = microalgos + account.PendingRewards
		// not implemented: account.Rewards sum of all rewards ever

		const nullarraystr = "[null]"

		if len(holdingAssetids) > 0 && string(holdingAssetids) != nullarraystr {
			var haids []uint64
			err = encoding.DecodeJSON(holdingAssetids, &haids)
			if err != nil {
				err = fmt.Errorf("parsing json holding asset ids err %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var hamounts []uint64
			err = encoding.DecodeJSON(holdingAmount, &hamounts)
			if err != nil {
				err = fmt.Errorf("parsing json holding amounts err %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var hfrozen []bool
			err = encoding.DecodeJSON(holdingFrozen, &hfrozen)
			if err != nil {
				err = fmt.Errorf("parsing json holding frozen err %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var holdingCreated []*uint64
			err = encoding.DecodeJSON(holdingCreatedBytes, &holdingCreated)
			if err != nil {
				err = fmt.Errorf("parsing json holding created ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var holdingClosed []*uint64
			err = encoding.DecodeJSON(holdingClosedBytes, &holdingClosed)
			if err != nil {
				err = fmt.Errorf("parsing json holding closed ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var holdingDeleted []*bool
			err = encoding.DecodeJSON(holdingDeletedBytes, &holdingDeleted)
			if err != nil {
				err = fmt.Errorf("parsing json holding deleted ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}

			if len(hamounts) != len(haids) || len(hfrozen) != len(haids) || len(holdingCreated) != len(haids) || len(holdingClosed) != len(haids) || len(holdingDeleted) != len(haids) {
				err = fmt.Errorf("account asset holding unpacking, all should be %d:  %d amounts, %d frozen, %d created, %d closed, %d deleted",
					len(haids), len(hamounts), len(hfrozen), len(holdingCreated), len(holdingClosed), len(holdingDeleted))
				req.out <- idb.AccountRow{Error: err}
				break
			}

			av := make([]models.AssetHolding, 0, len(haids))
			for i, assetid := range haids {
				// SQL can result in cross-product duplication when account has both asset holdings and assets created, de-dup here
				dup := false
				for _, xaid := range haids[:i] {
					if assetid == xaid {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				tah := models.AssetHolding{
					Amount:          hamounts[i],
					IsFrozen:        hfrozen[i],
					AssetId:         assetid,
					OptedOutAtRound: holdingClosed[i],
					OptedInAtRound:  holdingCreated[i],
					Deleted:         holdingDeleted[i],
				}
				av = append(av, tah)
			}
			account.Assets = new([]models.AssetHolding)
			*account.Assets = av
		}
		if len(assetParamsIds) > 0 && string(assetParamsIds) != nullarraystr {
			var assetids []uint64
			err = encoding.DecodeJSON(assetParamsIds, &assetids)
			if err != nil {
				err = fmt.Errorf("parsing json asset param ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			assetParams, err := encoding.DecodeAssetParamsArray(assetParamsStr)
			if err != nil {
				err = fmt.Errorf("parsing json asset param string, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var assetCreated []*uint64
			err = encoding.DecodeJSON(assetParamsCreatedBytes, &assetCreated)
			if err != nil {
				err = fmt.Errorf("parsing json asset created ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var assetClosed []*uint64
			err = encoding.DecodeJSON(assetParamsClosedBytes, &assetClosed)
			if err != nil {
				err = fmt.Errorf("parsing json asset closed ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var assetDeleted []*bool
			err = encoding.DecodeJSON(assetParamsDeletedBytes, &assetDeleted)
			if err != nil {
				err = fmt.Errorf("parsing json asset deleted ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}

			if len(assetParams) != len(assetids) || len(assetCreated) != len(assetids) || len(assetClosed) != len(assetids) || len(assetDeleted) != len(assetids) {
				err = fmt.Errorf("account asset unpacking, all should be %d:  %d assetids, %d created, %d closed, %d deleted",
					len(assetParams), len(assetids), len(assetCreated), len(assetClosed), len(assetDeleted))
				req.out <- idb.AccountRow{Error: err}
				break
			}

			cal := make([]models.Asset, 0, len(assetids))
			for i, assetid := range assetids {
				// SQL can result in cross-product duplication when account has both asset holdings and assets created, de-dup here
				dup := false
				for _, xaid := range assetids[:i] {
					if assetid == xaid {
						dup = true
						break
					}
				}
				if dup {
					continue
				}
				ap := assetParams[i]

				tma := models.Asset{
					Index:            assetid,
					CreatedAtRound:   assetCreated[i],
					DestroyedAtRound: assetClosed[i],
					Deleted:          assetDeleted[i],
					Params: models.AssetParams{
						Creator:       account.Address,
						Total:         ap.Total,
						Decimals:      uint64(ap.Decimals),
						DefaultFrozen: boolPtr(ap.DefaultFrozen),
						UnitName:      stringPtr(util.PrintableUTF8OrEmpty(ap.UnitName)),
						UnitNameB64:   byteSlicePtr([]byte(ap.UnitName)),
						Name:          stringPtr(util.PrintableUTF8OrEmpty(ap.AssetName)),
						NameB64:       byteSlicePtr([]byte(ap.AssetName)),
						Url:           stringPtr(util.PrintableUTF8OrEmpty(ap.URL)),
						UrlB64:        byteSlicePtr([]byte(ap.URL)),
						MetadataHash:  byteSliceOmitZeroPtr(ap.MetadataHash[:]),
						Manager:       addrStr(ap.Manager),
						Reserve:       addrStr(ap.Reserve),
						Freeze:        addrStr(ap.Freeze),
						Clawback:      addrStr(ap.Clawback),
					},
				}
				cal = append(cal, tma)
			}
			account.CreatedAssets = new([]models.Asset)
			*account.CreatedAssets = cal
		}

		if len(appParamIndexes) > 0 {
			// apps owned by this account
			var appIds []uint64
			err = encoding.DecodeJSON(appParamIndexes, &appIds)
			if err != nil {
				err = fmt.Errorf("parsing json appids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var appCreated []*uint64
			err = encoding.DecodeJSON(appCreatedBytes, &appCreated)
			if err != nil {
				err = fmt.Errorf("parsing json app created ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var appClosed []*uint64
			err = encoding.DecodeJSON(appClosedBytes, &appClosed)
			if err != nil {
				err = fmt.Errorf("parsing json app closed ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var appDeleted []*bool
			err = encoding.DecodeJSON(appDeletedBytes, &appDeleted)
			if err != nil {
				err = fmt.Errorf("parsing json app deleted flags, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}

			apps, err := encoding.DecodeAppParamsArray(appParams)
			if err != nil {
				err = fmt.Errorf("parsing json appparams, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			if len(appIds) != len(apps) || len(appClosed) != len(apps) || len(appCreated) != len(apps) || len(appDeleted) != len(apps) {
				err = fmt.Errorf("account app unpacking, all should be %d:  %d appids, %d appClosed, %d appCreated, %d appDeleted", len(apps), len(appIds), len(appClosed), len(appCreated), len(appDeleted))
				req.out <- idb.AccountRow{Error: err}
				break
			}

			aout := make([]models.Application, len(appIds))
			outpos := 0
			for i, appid := range appIds {
				aout[outpos].Id = appid
				aout[outpos].CreatedAtRound = appCreated[i]
				aout[outpos].DeletedAtRound = appClosed[i]
				aout[outpos].Deleted = appDeleted[i]
				aout[outpos].Params.Creator = &account.Address

				// If these are both nil the app was probably deleted, leave out params
				// some "required" fields will be left in the results.
				if apps[i].ApprovalProgram != nil || apps[i].ClearStateProgram != nil {
					aout[outpos].Params.ApprovalProgram = apps[i].ApprovalProgram
					aout[outpos].Params.ClearStateProgram = apps[i].ClearStateProgram
					aout[outpos].Params.GlobalState = tealKeyValueToModel(apps[i].GlobalState)
					aout[outpos].Params.GlobalStateSchema = &models.ApplicationStateSchema{
						NumByteSlice: apps[i].GlobalStateSchema.NumByteSlice,
						NumUint:      apps[i].GlobalStateSchema.NumUint,
					}
					aout[outpos].Params.LocalStateSchema = &models.ApplicationStateSchema{
						NumByteSlice: apps[i].LocalStateSchema.NumByteSlice,
						NumUint:      apps[i].LocalStateSchema.NumUint,
					}
					if apps[i].ExtraProgramPages > 0 {
						epp := uint64(apps[i].ExtraProgramPages)
						aout[outpos].Params.ExtraProgramPages = &epp
					}
				}

				outpos++
			}
			if outpos != len(aout) {
				aout = aout[:outpos]
			}
			account.CreatedApps = &aout
		}

		if len(localStateAppIds) > 0 {
			var appIds []uint64
			err = encoding.DecodeJSON(localStateAppIds, &appIds)
			if err != nil {
				err = fmt.Errorf("parsing json local appids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var appCreated []*uint64
			err = encoding.DecodeJSON(localStateCreatedBytes, &appCreated)
			if err != nil {
				err = fmt.Errorf("parsing json ls created ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var appClosed []*uint64
			err = encoding.DecodeJSON(localStateClosedBytes, &appClosed)
			if err != nil {
				err = fmt.Errorf("parsing json ls closed ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			var appDeleted []*bool
			err = encoding.DecodeJSON(localStateDeletedBytes, &appDeleted)
			if err != nil {
				err = fmt.Errorf("parsing json ls closed ids, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			ls, err := encoding.DecodeAppLocalStateArray(localStates)
			if err != nil {
				err = fmt.Errorf("parsing json local states, %v", err)
				req.out <- idb.AccountRow{Error: err}
				break
			}
			if len(appIds) != len(ls) || len(appClosed) != len(ls) || len(appCreated) != len(ls) || len(appDeleted) != len(ls) {
				err = fmt.Errorf("account app unpacking, all should be %d:  %d appids, %d appClosed, %d appCreated, %d appDeleted", len(ls), len(appIds), len(appClosed), len(appCreated), len(appDeleted))
				req.out <- idb.AccountRow{Error: err}
				break
			}

			aout := make([]models.ApplicationLocalState, len(ls))
			for i, appid := range appIds {
				aout[i].Id = appid
				aout[i].OptedInAtRound = appCreated[i]
				aout[i].ClosedOutAtRound = appClosed[i]
				aout[i].Deleted = appDeleted[i]
				aout[i].Schema = models.ApplicationStateSchema{
					NumByteSlice: ls[i].Schema.NumByteSlice,
					NumUint:      ls[i].Schema.NumUint,
				}
				aout[i].KeyValue = tealKeyValueToModel(ls[i].KeyValue)
			}
			account.AppsLocalState = &aout
		}

		req.out <- idb.AccountRow{Account: account}
		count++
		if req.opts.Limit != 0 && count >= req.opts.Limit {
			return
		}
	}
	if err := req.rows.Err(); err != nil {
		err = fmt.Errorf("error reading rows: %v", err)
		req.out <- idb.AccountRow{Error: err}
	}
}

func (db *IndexerDb) checkAccountResourceLimit(ctx context.Context, tx pgx.Tx, opts idb.AccountQueryOptions) error {
	// skip check if no resources are requested
	if !opts.IncludeAssetHoldings && !opts.IncludeAssetParams && !opts.IncludeAppLocalState && !opts.IncludeAppParams {
		return nil
	}

	// make a copy of the filters requested
	o := opts
	var countOnly bool

	if opts.IncludeDeleted {
		// if IncludeDeleted is set, need to construct a query (preserving filters) to count deleted values that would be returned from
		// asset, app, account_asset, account_app
		countOnly = true
	} else {
		// if IncludeDeleted is not set, query AccountData with no resources (preserving filters), to read ad.TotalX counts inside
		o.IncludeAssetHoldings = false
		o.IncludeAssetParams = false
		o.IncludeAppLocalState = false
		o.IncludeAppParams = false
	}

	query, whereArgs := db.buildAccountQuery(o, countOnly)
	rows, err := tx.Query(ctx, query, whereArgs...)
	if err != nil {
		return fmt.Errorf("account limit query %#v err %v", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		var addr []byte
		var microalgos uint64
		var rewardstotal uint64
		var createdat sql.NullInt64
		var closedat sql.NullInt64
		var deleted sql.NullBool
		var rewardsbase uint64
		var keytype *string
		var accountDataJSONStr []byte
		var holdingCount, assetCount, appCount, lsCount sql.NullInt64
		cols := []interface{}{&addr, &microalgos, &rewardstotal, &createdat, &closedat, &deleted, &rewardsbase, &keytype, &accountDataJSONStr}
		if countOnly {
			if o.IncludeAssetHoldings {
				cols = append(cols, &holdingCount)
			}
			if o.IncludeAssetParams {
				cols = append(cols, &assetCount)
			}
			if o.IncludeAppParams {
				cols = append(cols, &appCount)
			}
			if o.IncludeAppLocalState {
				cols = append(cols, &lsCount)
			}
		}
		err := rows.Scan(cols...)
		if err != nil {
			return fmt.Errorf("account limit scan err %v", err)
		}

		var ad ledgercore.AccountData
		ad, err = encoding.DecodeTrimmedLcAccountData(accountDataJSONStr)
		if err != nil {
			return fmt.Errorf("account limit decode err (%s) %v", accountDataJSONStr, err)
		}

		// check limit against filters (only count what would be returned)
		var resultCount, totalAssets, totalAssetParams, totalAppLocalStates, totalAppParams uint64
		if countOnly {
			totalAssets = uint64(holdingCount.Int64)
			totalAssetParams = uint64(assetCount.Int64)
			totalAppLocalStates = uint64(lsCount.Int64)
			totalAppParams = uint64(appCount.Int64)
		} else {
			totalAssets = ad.TotalAssets
			totalAssetParams = ad.TotalAssetParams
			totalAppLocalStates = ad.TotalAppLocalStates
			totalAppParams = ad.TotalAppParams
		}
		if opts.IncludeAssetHoldings {
			resultCount += totalAssets
		}
		if opts.IncludeAssetParams {
			resultCount += totalAssetParams
		}
		if opts.IncludeAppLocalState {
			resultCount += totalAppLocalStates
		}
		if opts.IncludeAppParams {
			resultCount += totalAppParams
		}
		if resultCount > opts.MaxResources {
			var aaddr basics.Address
			copy(aaddr[:], addr)
			return idb.MaxAPIResourcesPerAccountError{
				Address:             aaddr,
				TotalAppLocalStates: totalAppLocalStates,
				TotalAppParams:      totalAppParams,
				TotalAssets:         totalAssets,
				TotalAssetParams:    totalAssetParams,
			}
		}
	}
	return nil
}

func (db *IndexerDb) buildAccountQuery(opts idb.AccountQueryOptions, countOnly bool) (query string, whereArgs []interface{}) {
	// Construct query for fetching accounts...
	const maxWhereParts = 9
	whereParts := make([]string, 0, maxWhereParts)
	whereArgs = make([]interface{}, 0, maxWhereParts)
	partNumber := 1
	withClauses := make([]string, 0, maxWhereParts)
	// filter by has-asset or has-app
	if opts.HasAssetID != 0 {
		aq := fmt.Sprintf("SELECT addr FROM account_asset WHERE assetid = $%d", partNumber)
		whereArgs = append(whereArgs, opts.HasAssetID)
		partNumber++
		if opts.AssetGT != nil {
			aq += fmt.Sprintf(" AND amount > $%d", partNumber)
			whereArgs = append(whereArgs, *opts.AssetGT)
			partNumber++
		}
		if opts.AssetLT != nil {
			aq += fmt.Sprintf(" AND amount < $%d", partNumber)
			whereArgs = append(whereArgs, *opts.AssetLT)
			partNumber++
		}
		aq = "qasf AS (" + aq + ")"
		withClauses = append(withClauses, aq)
	}
	if opts.HasAppID != 0 {
		withClauses = append(withClauses, fmt.Sprintf("qapf AS (SELECT addr FROM account_app WHERE app = $%d)", partNumber))
		whereArgs = append(whereArgs, opts.HasAppID)
		partNumber++
	}
	// filters against main account table
	if len(opts.GreaterThanAddress) > 0 {
		whereParts = append(whereParts, fmt.Sprintf("a.addr > $%d", partNumber))
		whereArgs = append(whereArgs, opts.GreaterThanAddress)
		partNumber++
	}
	if len(opts.EqualToAddress) > 0 {
		whereParts = append(whereParts, fmt.Sprintf("a.addr = $%d", partNumber))
		whereArgs = append(whereArgs, opts.EqualToAddress)
		partNumber++
	}
	if opts.AlgosGreaterThan != nil {
		whereParts = append(whereParts, fmt.Sprintf("a.microalgos > $%d", partNumber))
		whereArgs = append(whereArgs, *opts.AlgosGreaterThan)
		partNumber++
	}
	if opts.AlgosLessThan != nil {
		whereParts = append(whereParts, fmt.Sprintf("a.microalgos < $%d", partNumber))
		whereArgs = append(whereArgs, *opts.AlgosLessThan)
		partNumber++
	}
	if !opts.IncludeDeleted {
		whereParts = append(whereParts, "NOT a.deleted")
	}
	if len(opts.EqualToAuthAddr) > 0 {
		whereParts = append(whereParts, fmt.Sprintf("a.account_data ->> 'spend' = $%d", partNumber))
		whereArgs = append(whereArgs, encoding.Base64(opts.EqualToAuthAddr))
		partNumber++
	}
	query = `SELECT a.addr, a.microalgos, a.rewards_total, a.created_at, a.closed_at, a.deleted, a.rewardsbase, a.keytype, a.account_data FROM account a`
	if opts.HasAssetID != 0 {
		// inner join requires match, filtering on presence of asset
		query += " JOIN qasf ON a.addr = qasf.addr"
	}
	if opts.HasAppID != 0 {
		// inner join requires match, filtering on presence of app
		query += " JOIN qapf ON a.addr = qapf.addr"
	}
	if len(whereParts) > 0 {
		whereStr := strings.Join(whereParts, " AND ")
		query += " WHERE " + whereStr
	}
	query += " ORDER BY a.addr ASC"
	if opts.Limit != 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}

	withClauses = append(withClauses, "qaccounts AS ("+query+")")
	query = "WITH " + strings.Join(withClauses, ", ")

	// build nested selects for querying app/asset data associated with an address
	if opts.IncludeAssetHoldings {
		var where, selectCols string
		if !opts.IncludeDeleted {
			where = ` WHERE NOT aa.deleted`
		}
		if countOnly {
			selectCols = `count(*) as holding_count`
		} else {
			selectCols = `json_agg(aa.assetid) as haid, json_agg(aa.amount) as hamt, json_agg(aa.frozen) as hf, json_agg(aa.created_at) as holding_created_at, json_agg(aa.closed_at) as holding_closed_at, json_agg(aa.deleted) as holding_deleted`
		}
		query += `, qaa AS (SELECT xa.addr, ` + selectCols + ` FROM account_asset aa JOIN qaccounts xa ON aa.addr = xa.addr` + where + ` GROUP BY 1)`
	}
	if opts.IncludeAssetParams {
		var where, selectCols string
		if !opts.IncludeDeleted {
			where = ` WHERE NOT ap.deleted`
		}
		if countOnly {
			selectCols = `count(*) as asset_count`
		} else {
			selectCols = `json_agg(ap.id) as paid, json_agg(ap.params) as pp, json_agg(ap.created_at) as asset_created_at, json_agg(ap.closed_at) as asset_closed_at, json_agg(ap.deleted) as asset_deleted`
		}
		query += `, qap AS (SELECT ya.addr, ` + selectCols + ` FROM asset ap JOIN qaccounts ya ON ap.creator_addr = ya.addr` + where + ` GROUP BY 1)`
	}
	if opts.IncludeAppParams {
		var where, selectCols string
		if !opts.IncludeDeleted {
			where = ` WHERE NOT app.deleted`
		}
		if countOnly {
			selectCols = `count(*) as app_count`
		} else {
			selectCols = `json_agg(app.id) as papps, json_agg(app.params) as ppa, json_agg(app.created_at) as app_created_at, json_agg(app.closed_at) as app_closed_at, json_agg(app.deleted) as app_deleted`
		}
		query += `, qapp AS (SELECT app.creator as addr, ` + selectCols + ` FROM app JOIN qaccounts ON qaccounts.addr = app.creator` + where + ` GROUP BY 1)`
	}
	if opts.IncludeAppLocalState {
		var where, selectCols string
		if !opts.IncludeDeleted {
			where = ` WHERE NOT la.deleted`
		}
		if countOnly {
			selectCols = `count(*) as ls_count`
		} else {
			selectCols = `json_agg(la.app) as lsapps, json_agg(la.localstate) as lsls, json_agg(la.created_at) as ls_created_at, json_agg(la.closed_at) as ls_closed_at, json_agg(la.deleted) as ls_deleted`
		}
		query += `, qls AS (SELECT la.addr, ` + selectCols + ` FROM account_app la JOIN qaccounts ON qaccounts.addr = la.addr` + where + ` GROUP BY 1)`
	}

	// query results
	query += ` SELECT za.addr, za.microalgos, za.rewards_total, za.created_at, za.closed_at, za.deleted, za.rewardsbase, za.keytype, za.account_data`
	if opts.IncludeAssetHoldings {
		if countOnly {
			query += `, qaa.holding_count`
		} else {
			query += `, qaa.haid, qaa.hamt, qaa.hf, qaa.holding_created_at, qaa.holding_closed_at, qaa.holding_deleted`
		}
	}
	if opts.IncludeAssetParams {
		if countOnly {
			query += `, qap.asset_count`
		} else {
			query += `, qap.paid, qap.pp, qap.asset_created_at, qap.asset_closed_at, qap.asset_deleted`
		}
	}
	if opts.IncludeAppParams {
		if countOnly {
			query += `, qapp.app_count`
		} else {
			query += `, qapp.papps, qapp.ppa, qapp.app_created_at, qapp.app_closed_at, qapp.app_deleted`
		}
	}
	if opts.IncludeAppLocalState {
		if countOnly {
			query += `, qls.ls_count`
		} else {
			query += `, qls.lsapps, qls.lsls, qls.ls_created_at, qls.ls_closed_at, qls.ls_deleted`
		}
	}
	query += ` FROM qaccounts za`

	// join everything together
	if opts.IncludeAssetHoldings {
		query += ` LEFT JOIN qaa ON za.addr = qaa.addr`
	}
	if opts.IncludeAssetParams {
		query += ` LEFT JOIN qap ON za.addr = qap.addr`
	}
	if opts.IncludeAppParams {
		query += ` LEFT JOIN qapp ON za.addr = qapp.addr`
	}
	if opts.IncludeAppLocalState {
		query += ` LEFT JOIN qls ON za.addr = qls.addr`
	}
	query += ` ORDER BY za.addr ASC;`
	return query, whereArgs
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
	migrationRequired := false
	migrating := false
	blocking := false
	errString := ""
	var data = make(map[string]interface{})

	if db.readonly {
		data["read-only-mode"] = true
	}

	//if db.migration != nil {
	//	state := db.migration.GetStatus()
	//
	//	if state.Err != nil {
	//		errString = state.Err.Error()
	//	}
	//	if state.Status != "" {
	//		data["migration-status"] = state.Status
	//	}
	//
	//	migrationRequired = state.Running
	//	migrating = state.Running
	//	blocking = state.Blocking
	//} else {
	//	state, err := db.getMigrationState(ctx, nil)
	//	if err != nil {
	//		return idb.Health{}, err
	//	}
	//
	//	blocking = migrationStateBlocked(state)
	//	migrationRequired = needsMigration(state)
	//}

	data["migration-required"] = migrationRequired

	round, err := db.getMaxRoundAccounted(ctx, nil)

	// We'll just have to set the round to 0
	if err == idb.ErrorNotInitialized {
		err = nil
		round = 0
	}

	return idb.Health{
		Data:        &data,
		Round:       round,
		IsMigrating: migrating,
		DBAvailable: !blocking,
		Error:       errString,
	}, err
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

// DeleteTransactions is part of idb.IndexerDB
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

// Returns ErrorNotInitialized if genesis is not loaded.
// If `tx` is nil, use a normal query.
func (db *IndexerDb) getMaxRoundAccounted(ctx context.Context, tx pgx.Tx) (uint64, error) {
	round, err := db.getNextRoundToAccount(ctx, tx)
	if err != nil {
		return 0, err
	}

	if round > 0 {
		round--
	}
	return round, nil
}
func nullableInt64Ptr(x sql.NullInt64) *uint64 {
	if !x.Valid {
		return nil
	}
	return uint64Ptr(uint64(x.Int64))
}

func nullableBoolPtr(x sql.NullBool) *bool {
	if !x.Valid {
		return nil
	}
	return &x.Bool
}

func uintOrDefault(x *uint64) uint64 {
	if x != nil {
		return *x
	}
	return 0
}

func uint64Ptr(x uint64) *uint64 {
	out := new(uint64)
	*out = x
	return out
}

func boolPtr(x bool) *bool {
	out := new(bool)
	*out = x
	return out
}

func stringPtr(x string) *string {
	if len(x) == 0 {
		return nil
	}
	out := new(string)
	*out = x
	return out
}

func byteSlicePtr(x []byte) *[]byte {
	if len(x) == 0 {
		return nil
	}

	xx := make([]byte, len(x))
	copy(xx, x)
	return &xx
}

func byteSliceOmitZeroPtr(x []byte) *[]byte {
	allzero := true
	for _, b := range x {
		if b != 0 {
			allzero = false
			break
		}
	}
	if allzero {
		return nil
	}

	xx := make([]byte, len(x))
	copy(xx, x)
	return &xx
}

func allZero(x []byte) bool {
	for _, v := range x {
		if v != 0 {
			return false
		}
	}
	return true
}

func addrStr(addr sdk.Address) *string {
	if addr.IsZero() {
		return nil
	}
	out := new(string)
	*out = addr.String()
	return out
}
