package mem

import (
	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/dbadapter"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/types"
)

var (
	_ types.KVStore   = (*Store)(nil)
	_ types.Committer = (*Store)(nil)
)

// Store implements an in-memory only KVStore. Entries are persisted between
// commits and thus between blocks. State in Memory store is not committed as part of app state but maintained privately by each node
type Store struct {
	dbadapter.Store
}

func NewStore() *Store {
	return NewStoreWithDB(dbm.NewMemDB())
}

func NewStoreWithDB(db *dbm.MemDB) *Store { //nolint: interfacer // Concrete return type is fine here.
	return &Store{Store: dbadapter.Store{DB: db}}
}

// GetStoreType returns the Store's type.
func (s Store) GetStoreType() types.StoreType {
	return types.StoreTypeMemory
}

// CacheWrap branches the underlying store.
func (s Store) CacheWrap() types.CacheWrap {
	return cachekv.NewStore(s)
}

// Commit performs a no-op as entries are persistent between commitments.
func (s *Store) Commit() (id types.CommitID) { return }

func (s *Store) SetPruning(pruning pruningtypes.PruningOptions) {}

// GetPruning is a no-op as pruning options cannot be directly set on this store.
// They must be set on the root commit multi-store.
func (s *Store) GetPruning() pruningtypes.PruningOptions {
	return pruningtypes.NewPruningOptions(pruningtypes.PruningUndefined)
}

func (s Store) LastCommitID() (id types.CommitID) { return }

func (s Store) WorkingHash() (hash []byte) { return }
