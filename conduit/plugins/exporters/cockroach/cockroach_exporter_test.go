package cockroach

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"

	"github.com/algorand/go-algorand/agreement"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-algorand/ledger/ledgercore"

	"github.com/algorand/indexer/conduit/plugins"
	"github.com/algorand/indexer/conduit/plugins/exporters"
	"github.com/algorand/indexer/data"
	_ "github.com/algorand/indexer/idb/dummy"
	testutil "github.com/algorand/indexer/util/test"

	sdk "github.com/algorand/go-algorand-sdk/types"
)

var cockroachConstructor = exporters.ExporterConstructorFunc(func() exporters.Exporter {
	return &cockroachdbExporter{}
})
var logger *logrus.Logger
var round = basics.Round(0)

func init() {
	logger, _ = test.NewNullLogger()
}

func TestExporterMetadata(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	meta := cockroachExp.Metadata()
	assert.Equal(t, metadata.Name, meta.Name)
	assert.Equal(t, metadata.Description, meta.Description)
	assert.Equal(t, metadata.Deprecated, meta.Deprecated)
}

func TestConnectDisconnectSuccess(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true\nconnection-string: ''")
	assert.NoError(t, cockroachExp.Init(context.Background(), testutil.MockedInitProvider(&round), cfg, logger))
	assert.NoError(t, cockroachExp.Close())
}

func TestConnectUnmarshalFailure(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("'")
	assert.ErrorContains(t, cockroachExp.Init(context.Background(), testutil.MockedInitProvider(&round), cfg, logger), "connect failure in unmarshalConfig")
}

func TestConnectDbFailure(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("")
	assert.ErrorContains(t, cockroachExp.Init(context.Background(), testutil.MockedInitProvider(&round), cfg, logger), "connection string is empty for cockroachdb")
}

func TestConfigDefault(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	defaultConfig := &ExporterConfig{}
	expected, err := yaml.Marshal(defaultConfig)
	if err != nil {
		t.Fatalf("unable to Marshal default Cockroach.ExporterConfig: %v", err)
	}
	assert.Equal(t, string(expected), cockroachExp.Config())
}

func TestReceiveInvalidBlock(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true")
	assert.NoError(t, cockroachExp.Init(context.Background(), testutil.MockedInitProvider(&round), cfg, logger))

	invalidBlock := data.BlockData{
		BlockHeader: bookkeeping.BlockHeader{
			Round: 1,
		},
		Payset:      transactions.Payset{},
		Certificate: &agreement.Certificate{},
		Delta:       nil,
	}
	expectedErr := fmt.Sprintf("receive got an invalid block: %#v", invalidBlock)
	assert.EqualError(t, cockroachExp.Receive(invalidBlock), expectedErr)
}

func TestReceiveAddBlockSuccess(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true")
	assert.NoError(t, cockroachExp.Init(context.Background(), testutil.MockedInitProvider(&round), cfg, logger))

	block := data.BlockData{
		BlockHeader: bookkeeping.BlockHeader{},
		Payset:      transactions.Payset{},
		Certificate: &agreement.Certificate{},
		Delta:       &ledgercore.StateDelta{},
	}
	assert.NoError(t, cockroachExp.Receive(block))
}

func TestCockroachExporterInit(t *testing.T) {
	cockroachExp := cockroachConstructor.New()
	cfg := plugins.MakePluginConfig("test: true")

	// genesis hash mismatch
	initProvider := testutil.MockedInitProvider(&round)
	initProvider.Genesis = &sdk.Genesis{
		Network: "test",
	}
	err := cockroachExp.Init(context.Background(), initProvider, cfg, logger)
	assert.Contains(t, err.Error(), "error importing genesis: genesis hash not matching")

	// incorrect round
	round = 1
	err = cockroachExp.Init(context.Background(), testutil.MockedInitProvider(&round), cfg, logger)
	assert.Contains(t, err.Error(), "initializing block round 1 but next round to account is 0")
}
