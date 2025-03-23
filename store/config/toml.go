package config

// DefaultConfigTemplate defines the configuration template for the memiavl configuration
const DefaultConfigTemplate = `
###############################################################################
###                             MemIAVL Configuration                       ###
###############################################################################

[memiavl]

# Enable defines if the memiavl should be enabled.
enable = {{ .MemIAVL.Enable }}

# ZeroCopy defines if the memiavl should return slices pointing to mmap-ed buffers directly (zero-copy),
# the zero-copied slices must not be retained beyond current block's execution.
# the sdk address cache will be disabled if zero-copy is enabled.
zero-copy = {{ .MemIAVL.ZeroCopy }}

# AsyncCommitBuffer defines the size of asynchronous commit queue, this greatly improve block catching-up
# performance, -1 means synchronous commit.
async-commit-buffer = {{ .MemIAVL.AsyncCommitBuffer }}

# SnapshotKeepRecent defines what many old snapshots (excluding the latest one) to keep after new snapshots are
# taken, defaults to 1 to make sure ibc relayers work.
snapshot-keep-recent = {{ .MemIAVL.SnapshotKeepRecent }}

# SnapshotInterval defines the block interval the memiavl snapshot is taken, default to 1000.
snapshot-interval = {{ .MemIAVL.SnapshotInterval }}

# CacheSize defines the size of the cache for each memiavl store, default to 1000.
cache-size = {{ .MemIAVL.CacheSize }}

# MaxCatchupTimes defines the max times of catching up WALs async
# can not less than 1, default to 5
max-catchup-times = {{ .MemIAVL.MaxCatchupTimes }}

# WalLagThreshold determine whether to proceed with the next round of catchupWAL, 
# pass to main thread if less than WalLagThreshold
# default to 10, 
# equals to set max-catchup-times=1 if set wal-lag-threshold a large number such as the MaxUint64(18446744073709551615)
wal-lag-threshold = {{ .MemIAVL.WalLagThreshold }}
`
