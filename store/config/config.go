package config

import "github.com/crypto-org-chain/cronos/memiavl"

const DefaultCacheSize = 1000

type MemIAVLConfig struct {
	// Enable defines if the memiavl should be enabled.
	Enable bool `mapstructure:"enable"`
	// ZeroCopy defines if the memiavl should return slices pointing to mmap-ed buffers directly (zero-copy),
	// the zero-copied slices must not be retained beyond current block's execution.
	// the sdk address cache will be disabled if zero-copy is enabled.
	ZeroCopy bool `mapstructure:"zero-copy"`
	// AsyncCommitBuffer defines the size of asynchronous commit queue, this greatly improve block catching-up
	// performance, -1 means synchronous commit.
	AsyncCommitBuffer int `mapstructure:"async-commit-buffer"`
	// SnapshotKeepRecent defines what many old snapshots (excluding the latest one) to keep after new snapshots are
	// taken, defaults to 1 to make sure ibc relayers work.
	SnapshotKeepRecent uint32 `mapstructure:"snapshot-keep-recent"`
	// SnapshotInterval defines the block interval the memiavl snapshot is taken, default to 1000.
	SnapshotInterval uint32 `mapstructure:"snapshot-interval"`
	// CacheSize defines the size of the cache for each memiavl store.
	CacheSize int `mapstructure:"cache-size"`
	//  WalReaders defines the number of concurrent readers for WAL.
	WalReaders int `mapstructure:"wal-readers"`
	// MaxCatchupTimes defines the max times of catching up WALs async,
	// can not less than 1.
	MaxCatchupTimes int `mapstructure:"max-catchup-times"`
	// WalLagThreshold determine whether to proceed with the next round of catchupWAL,
	// pass to main thread if less than WalLagThreshold.
	WalLagThreshold uint64 `mapstructure:"wal-lag-threshold"`
}

func DefaultMemIAVLConfig() MemIAVLConfig {
	return MemIAVLConfig{
		CacheSize:          DefaultCacheSize,
		SnapshotInterval:   memiavl.DefaultSnapshotInterval,
		SnapshotKeepRecent: 1,
		WalReaders:         memiavl.DefaultWalReaders,
		MaxCatchupTimes:    memiavl.DefaultMaxCatchupTimes,
		WalLagThreshold:    memiavl.DefaultWalLagThreshold,
	}
}
