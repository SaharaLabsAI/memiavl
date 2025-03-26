package rocksdb

import (
	"errors"
	"runtime"
	"strings"

	"github.com/linxGnu/grocksdb"

	dbm "github.com/cosmos/cosmos-db"
)

const DefaultBlockCacheSize = 1 << 30

type Mode int

const (
	ReadWrite Mode = iota
	ReadOnly
)

type Options struct {
	Rocks          *grocksdb.Options
	BlockCacheSize uint64
}

func Open(dir string, mode Mode) (dbm.DB, error) {
	opts, err := loadLatestOptions(dir)
	if err != nil {
		return nil, err
	}
	// customize rocksdb options
	opts = NewOptions(opts, false)

	var db *grocksdb.DB
	switch mode {
	case ReadWrite:
		db, err = grocksdb.OpenDbForReadOnly(opts.Rocks, dir, false)
	case ReadOnly:
		db, err = grocksdb.OpenDb(opts.Rocks, dir)
	default:
		db, err = nil, errors.New("unknown rocksdb mode")
	}
	if err != nil {
		return nil, err
	}

	ro := grocksdb.NewDefaultReadOptions()
	wo := grocksdb.NewDefaultWriteOptions()
	woSync := grocksdb.NewDefaultWriteOptions()
	woSync.SetSync(true)

	return dbm.NewRocksDBWithRawDB(db, ro, wo, woSync), nil
}

// NewOptions build options for `application.db`,
// it overrides existing options if provided, otherwise create new one assuming it's a new database.
func NewOptions(opts *Options, sstFileWriter bool) *Options {
	if opts == nil {
		opts = &Options{
			Rocks: grocksdb.NewDefaultOptions(),
		}
		// only enable dynamic-level-bytes on new db, don't override for existing db
		opts.Rocks.SetLevelCompactionDynamicLevelBytes(true)
	}
	opts.Rocks.SetCreateIfMissing(true)
	opts.Rocks.IncreaseParallelism(runtime.NumCPU())
	opts.Rocks.OptimizeLevelStyleCompaction(512 * 1024 * 1024)
	opts.Rocks.SetTargetFileSizeMultiplier(2)

	// block based table options
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()

	// 1G block cache
	bbto.SetBlockCache(grocksdb.NewLRUCache(DefaultBlockCacheSize))

	// http://rocksdb.org/blog/2021/12/29/ribbon-filter.html
	bbto.SetFilterPolicy(grocksdb.NewRibbonHybridFilterPolicy(9.9, 1))

	// partition index
	// http://rocksdb.org/blog/2017/05/12/partitioned-index-filter.html
	bbto.SetIndexType(grocksdb.KTwoLevelIndexSearchIndexType)
	bbto.SetPartitionFilters(true)

	// hash index is better for iavl tree which mostly do point lookup.
	bbto.SetDataBlockIndexType(grocksdb.KDataBlockIndexTypeBinarySearchAndHash)

	opts.Rocks.SetBlockBasedTableFactory(bbto)

	// in iavl tree, we almost always query existing keys
	opts.Rocks.SetOptimizeFiltersForHits(true)

	// heavier compression option at bottommost level,
	// 110k dict bytes is default in zstd library,
	// train bytes is recommended to be set at 100x dict bytes.
	opts.Rocks.SetBottommostCompression(grocksdb.ZSTDCompression)
	compressOpts := grocksdb.NewDefaultCompressionOptions()
	compressOpts.Level = 12
	if !sstFileWriter {
		compressOpts.MaxDictBytes = 110 * 1024
		opts.Rocks.SetBottommostCompressionOptionsZstdMaxTrainBytes(compressOpts.MaxDictBytes*100, true)
	}
	opts.Rocks.SetBottommostCompressionOptions(compressOpts, true)

	return opts
}

// loadLatestOptions try to load options from existing db, returns nil if not exists.
func loadLatestOptions(dir string) (*Options, error) {
	opts, err := grocksdb.LoadLatestOptions(dir, grocksdb.NewDefaultEnv(), true, grocksdb.NewLRUCache(DefaultBlockCacheSize))
	if err != nil {
		// not found is not an error
		if strings.HasPrefix(err.Error(), "NotFound: ") {
			return nil, nil
		}
		return nil, err
	}

	cfNames := opts.ColumnFamilyNames()
	cfOpts := opts.ColumnFamilyOpts()

	for i := 0; i < len(cfNames); i++ {
		if cfNames[i] == "default" {
			return &Options{
				Rocks:          &cfOpts[i],
				BlockCacheSize: DefaultBlockCacheSize,
			}, nil
		}
	}

	return &Options{
		Rocks:          opts.Options(),
		BlockCacheSize: DefaultBlockCacheSize,
	}, nil
}
