package cockroach

import (
	log "github.com/sirupsen/logrus"

	"github.com/algorand/indexer/idb"
)

type cockroachFactory struct {
}

// Name is part of the IndexerFactory interface.
func (cf cockroachFactory) Name() string {
	return "cockroachdb"
}

// Build is part of the IndexerFactory interface.
func (cf cockroachFactory) Build(arg string, opts idb.IndexerDbOptions, log *log.Logger) (idb.IndexerDb, chan struct{}, error) {
	return Init(arg, opts, log)
}

func init() {
	idb.RegisterFactory("cockroachdb", &cockroachFactory{})
}
