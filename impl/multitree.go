package memiavl

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	"github.com/tidwall/wal"
	"golang.org/x/exp/slices"
)

const MetadataFileName = "__metadata"

type NamedTree struct {
	*Tree
	Name string
}

// MultiTree manages multiple memiavl tree together,
// all the trees share the same latest version, the snapshots are always created at the same version.
//
// The snapshot structure is like this:
// ```
// > snapshot-V
// >  metadata
// >  bank
// >   kvs
// >   nodes
// >   metadata
// >  acc
// >  other stores...
// ```
type MultiTree struct {
	// if the tree is start from genesis, it's the initial version of the chain,
	// if the tree is imported from snapshot, it's the imported version plus one,
	// it always corresponds to the wal entry with index 1.
	initialVersion uint32

	zeroCopy   bool
	cacheSize  int
	walReaders int

	trees          []NamedTree    // always ordered by tree name
	treesByName    map[string]int // index of the trees by name
	lastCommitInfo CommitInfo

	// the initial metadata loaded from disk snapshot
	metadata MultiTreeMetadata
}

func NewEmptyMultiTree(initialVersion uint32, cacheSize, walReaders int) *MultiTree {
	return &MultiTree{
		initialVersion: initialVersion,
		treesByName:    make(map[string]int),
		zeroCopy:       true,
		cacheSize:      cacheSize,
		walReaders:     walReaders,
	}
}

func LoadMultiTree(dir string, zeroCopy bool, cacheSize, walReaders int) (*MultiTree, error) {
	metadata, err := readMetadata(dir)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	treeMap := make(map[string]*Tree, len(entries))
	treeNames := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		treeNames = append(treeNames, name)
		snapshot, err := OpenSnapshot(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		treeMap[name] = NewFromSnapshot(snapshot, zeroCopy, cacheSize)
	}

	slices.Sort(treeNames)

	trees := make([]NamedTree, len(treeNames))
	treesByName := make(map[string]int, len(trees))
	for i, name := range treeNames {
		tree := treeMap[name]
		trees[i] = NamedTree{Tree: tree, Name: name}
		treesByName[name] = i
	}

	mtree := &MultiTree{
		trees:          trees,
		treesByName:    treesByName,
		lastCommitInfo: *metadata.CommitInfo,
		metadata:       *metadata,
		zeroCopy:       zeroCopy,
		cacheSize:      cacheSize,
		walReaders:     walReaders,
	}
	// initial version is nesserary for wal index conversion,
	// overflow checked in `readMetadata`.
	mtree.setInitialVersion(uint32(metadata.InitialVersion))
	return mtree, nil
}

// TreeByName returns the tree by name, returns nil if not found
func (t *MultiTree) TreeByName(name string) *Tree {
	if i, ok := t.treesByName[name]; ok {
		return t.trees[i].Tree
	}
	return nil
}

// Trees returns all the trees together with the name, ordered by name.
func (t *MultiTree) Trees() []NamedTree {
	return t.trees
}

func (t *MultiTree) SetInitialVersion(initialVersion int64) error {
	if initialVersion >= math.MaxUint32 {
		return fmt.Errorf("version overflows uint32: %d", initialVersion)
	}

	if t.Version() != 0 {
		return fmt.Errorf("multi tree is not empty: %d", t.Version())
	}

	for _, entry := range t.trees {
		if !entry.IsEmpty() {
			return fmt.Errorf("tree is not empty: %s", entry.Name)
		}
	}

	t.setInitialVersion(uint32(initialVersion))
	return nil
}

func (t *MultiTree) setInitialVersion(initialVersion uint32) {
	t.initialVersion = initialVersion
	if t.initialVersion > 1 {
		for _, entry := range t.trees {
			entry.setInitialVersion(t.initialVersion)
		}
	}
}

func (t *MultiTree) SetZeroCopy(zeroCopy bool) {
	t.zeroCopy = zeroCopy
	for _, entry := range t.trees {
		entry.SetZeroCopy(zeroCopy)
	}
}

// Copy returns a snapshot of the tree which won't be corrupted by further modifications on the main tree.
func (t *MultiTree) Copy(cacheSize int) *MultiTree {
	trees := make([]NamedTree, len(t.trees))
	treesByName := make(map[string]int, len(t.trees))
	for i, entry := range t.trees {
		tree := entry.Copy(cacheSize)
		trees[i] = NamedTree{Tree: tree, Name: entry.Name}
		treesByName[entry.Name] = i
	}

	clone := *t
	clone.trees = trees
	clone.treesByName = treesByName
	return &clone
}

func (t *MultiTree) Version() int64 {
	return t.lastCommitInfo.Version
}

func (t *MultiTree) SnapshotVersion() int64 {
	return t.metadata.CommitInfo.Version
}

func (t *MultiTree) LastCommitInfo() *CommitInfo {
	return &t.lastCommitInfo
}

func (t *MultiTree) applyWALEntry(entry WALEntry) error {
	if err := t.ApplyUpgrades(entry.Upgrades); err != nil {
		return err
	}
	return t.ApplyChangeSets(entry.Changesets)
}

// ApplyUpgrades store name upgrades
func (t *MultiTree) ApplyUpgrades(upgrades []*TreeNameUpgrade) error {
	if len(upgrades) == 0 {
		return nil
	}

	t.treesByName = nil // rebuild in the end

	for _, upgrade := range upgrades {
		switch {
		case upgrade.Delete:
			i := slices.IndexFunc(t.trees, func(entry NamedTree) bool {
				return entry.Name == upgrade.Name
			})
			if i < 0 {
				return fmt.Errorf("unknown tree name %s", upgrade.Name)
			}
			// swap deletion
			t.trees[i], t.trees[len(t.trees)-1] = t.trees[len(t.trees)-1], t.trees[i]
			t.trees = t.trees[:len(t.trees)-1]
		case upgrade.RenameFrom != "":
			// rename tree
			i := slices.IndexFunc(t.trees, func(entry NamedTree) bool {
				return entry.Name == upgrade.RenameFrom
			})
			if i < 0 {
				return fmt.Errorf("unknown tree name %s", upgrade.RenameFrom)
			}
			t.trees[i].Name = upgrade.Name
		default:
			// add tree
			tree := NewWithInitialVersion(uint32(nextVersion(t.Version(), t.initialVersion)), t.cacheSize)
			t.trees = append(t.trees, NamedTree{Tree: tree, Name: upgrade.Name})
		}
	}

	sort.SliceStable(t.trees, func(i, j int) bool {
		return t.trees[i].Name < t.trees[j].Name
	})
	t.treesByName = make(map[string]int, len(t.trees))
	for i, tree := range t.trees {
		if _, ok := t.treesByName[tree.Name]; ok {
			return fmt.Errorf("memiavl tree name conflicts: %s", tree.Name)
		}
		t.treesByName[tree.Name] = i
	}

	return nil
}

// ApplyChangeSet applies change set for a single tree.
func (t *MultiTree) ApplyChangeSet(name string, changeSet ChangeSet) error {
	i, found := t.treesByName[name]
	if !found {
		return fmt.Errorf("unknown tree name %s", name)
	}
	t.trees[i].ApplyChangeSet(changeSet)
	return nil
}

// ApplyChangeSets applies change sets for multiple trees.
func (t *MultiTree) ApplyChangeSets(changeSets []*NamedChangeSet) error {
	for _, cs := range changeSets {
		if err := t.ApplyChangeSet(cs.Name, cs.Changeset); err != nil {
			return err
		}
	}
	return nil
}

// WorkingCommitInfo returns the commit info for the working tree
func (t *MultiTree) WorkingCommitInfo() *CommitInfo {
	version := nextVersion(t.lastCommitInfo.Version, t.initialVersion)
	return t.buildCommitInfo(version)
}

// SaveVersion bumps the versions of all the stores and optionally returns the new app hash
func (t *MultiTree) SaveVersion(updateCommitInfo bool) (int64, error) {
	t.lastCommitInfo.Version = nextVersion(t.lastCommitInfo.Version, t.initialVersion)
	for _, entry := range t.trees {
		if _, _, err := entry.SaveVersion(updateCommitInfo); err != nil {
			return 0, err
		}
	}

	if updateCommitInfo {
		t.UpdateCommitInfo()
	} else {
		// clear the dirty information
		t.lastCommitInfo.StoreInfos = []StoreInfo{}
	}

	return t.lastCommitInfo.Version, nil
}

func (t *MultiTree) buildCommitInfo(version int64) *CommitInfo {
	fmt.Println("| MEM-IMPL | buildCommitInfo | start")
	var infos []StoreInfo
	for _, entry := range t.trees {
		fmt.Printf("| MEM-IMPL | buildCommitInfo |  entry.Name = %s, entry.Version = %d, entry.RootHash = %x\n", entry.Name, entry.Version(), entry.RootHash())
		infos = append(infos, StoreInfo{
			Name: entry.Name,
			CommitId: CommitID{
				Version: entry.Version(),
				Hash:    entry.RootHash(),
			},
		})
	}

	return &CommitInfo{
		Version:    version,
		StoreInfos: infos,
	}
}

// UpdateCommitInfo update lastCommitInfo based on current status of trees.
// it's needed if `updateCommitInfo` is set to `false` in `ApplyChangeSet`.
func (t *MultiTree) UpdateCommitInfo() {
	t.lastCommitInfo = *t.buildCommitInfo(t.lastCommitInfo.Version)
}

// CatchupWAL replay the new entries in the WAL on the tree to catch-up to the target or latest version.
func (t *MultiTree) CatchupWAL(wal *wal.Log, endVersion int64) error {
	lastIndex, err := wal.LastIndex()
	if err != nil {
		return fmt.Errorf("read wal last index failed, %w", err)
	}

	firstIndex := walIndex(nextVersion(t.Version(), t.initialVersion), t.initialVersion)
	if firstIndex > lastIndex {
		// already up-to-date
		return nil
	}

	endIndex := lastIndex
	if endVersion != 0 {
		endIndex = walIndex(endVersion, t.initialVersion)
	}
	fmt.Printf("|MEM-IMPL-MULTI| CatchupWAL | endVersion = %d, firstIndex = %d, lastIndex = %d, endIndex = %d\n", endVersion, firstIndex, lastIndex, endIndex)

	if endIndex < firstIndex {
		return fmt.Errorf("target index %d is pruned", endIndex)
	}

	if endIndex > lastIndex {
		return fmt.Errorf("target index %d is in the future, latest index: %d", endIndex, lastIndex)
	}

	return t.readWALs(wal, firstIndex, endIndex)
}

// GetCatchupWALRange get the range of wal index from the oldest one to the latest one
func (t *MultiTree) GetCatchupWALRange(wal *wal.Log) (uint64, uint64, error) {
	lastIndex, err := wal.LastIndex()
	if err != nil {
		return 0, 0, fmt.Errorf("read wal last index failed, %w", err)
	}
	fmt.Printf("|MEM-IMPL-MULTI| GetCatchupWALRange | t.Version() = %d, t.initialVersion = %d, nextVersion = %d\n", t.Version(), t.initialVersion, nextVersion(t.Version(), t.initialVersion))

	firstIndex := walIndex(nextVersion(t.Version(), t.initialVersion), t.initialVersion)

	fmt.Printf("|MEM-IMPL-MULTI| GetCatchupWALRange | firstIndex = %d, lastIndex = %d\n", firstIndex, lastIndex)
	if lastIndex < firstIndex {
		return 0, 0, fmt.Errorf("target index %d is pruned", lastIndex)
	}

	return firstIndex, lastIndex, nil
}

func (t *MultiTree) CatchupWALWithRange(wal *wal.Log, firstIndex, endIndex uint64) error {
	return t.readWALs(wal, firstIndex, endIndex)
}

func (t *MultiTree) readWALs(wal *wal.Log, firstIndex, endIndex uint64) error {
	fmt.Println("|MEM-MULTI | readWALs| walReaders = ", t.walReaders)
	if t.walReaders == 1 {
		return t.readWALsSequentially(wal, firstIndex, endIndex)
	}
	return t.readWALsConcurrently(wal, firstIndex, endIndex)
}

func (t *MultiTree) readWALsSequentially(wal *wal.Log, firstIndex, endIndex uint64) error {
	tm := time.Now()
	defer func() {
		fmt.Printf("|MEM-MULTI | readWALs| readWALsSequentially | walReaders = %d, firstIndex = %d, endIndex = %d, finish-cost-time = %d\n", t.walReaders, firstIndex, endIndex, time.Since(tm).Microseconds())
	}()
	for i := firstIndex; i <= endIndex; i++ {
		bz, err := wal.Read(i)
		if err != nil {
			return fmt.Errorf("read wal log failed, %w", err)
		}
		var entry WALEntry
		if err := entry.Unmarshal(bz); err != nil {
			return fmt.Errorf("unmarshal wal log failed, %w", err)
		}
		if err := t.applyWALEntry(entry); err != nil {
			return fmt.Errorf("replay wal entry failed, %w", err)
		}
		if _, err := t.SaveVersion(false); err != nil {
			return fmt.Errorf("replay change set failed, %w", err)
		}
	}
	t.UpdateCommitInfo()
	return nil
}

func (t *MultiTree) readWALsConcurrently(wal *wal.Log, firstIndex, endIndex uint64) error {
	tm := time.Now()
	defer func() {
		fmt.Printf("|MEM-MULTI | readWALs| readWALsConcurrently | walReaders = %d, firstIndex = %d, endIndex = %d, finish-cost-time = %d\n", t.walReaders, firstIndex, endIndex, time.Since(tm).Microseconds())
	}()

	type walItem struct {
		index uint64
		entry WALEntry
		err   error
	}

	readWorkers := t.walReaders
	ringBuffer := make([]walItem, readWorkers)
	for i := range ringBuffer {
		ringBuffer[i].index = math.MaxUint64
	}

	var (
		mu               sync.Mutex
		cond             = sync.NewCond(&mu) // to notify producers and consumers
		nextProcessIndex = firstIndex
		errCh            = make(chan error, 1)
		hasError         int32     // atomic to mark errors and avoid race conditions
		once             sync.Once // ensure the error is sent only once.
	)

	// run producers
	var workerWg sync.WaitGroup
	for w := 0; w < readWorkers; w++ {
		workerWg.Add(1)
		go func(workerId int) {
			defer workerWg.Done()

			readIndex := firstIndex + uint64(workerId)

			for readIndex <= endIndex {
				if atomic.LoadInt32(&hasError) == 1 {
					return
				}

				// read and unmarshal WAL
				bz, err := wal.Read(readIndex)
				var entry WALEntry
				if err == nil {
					err = entry.Unmarshal(bz)
				}

				mu.Lock()
				for ringBuffer[workerId].index != math.MaxUint64 && atomic.LoadInt32(&hasError) == 0 {
					cond.Wait() // wait consumer clear slot
				}

				if atomic.LoadInt32(&hasError) == 1 {
					mu.Unlock()
					return
				}

				ringBuffer[workerId] = walItem{
					index: readIndex,
					entry: entry,
					err:   err,
				}
				mu.Unlock()
				cond.Broadcast() // notice consumer

				readIndex += uint64(readWorkers)
			}
		}(w)
	}

	// start consumers
	var processWg sync.WaitGroup
	processWg.Add(1)
	go func() {
		defer processWg.Done()

		for nextProcessIndex <= endIndex {
			mu.Lock()
			workerIdx := int((nextProcessIndex - firstIndex) % uint64(readWorkers))
			item := ringBuffer[workerIdx]

			if item.index != nextProcessIndex {
				cond.Wait() // wait producer
				mu.Unlock()
				continue
			}

			// process WAL
			ringBuffer[workerIdx].index = math.MaxUint64
			mu.Unlock()
			cond.Broadcast() // notice producer to read next one

			if item.err != nil {
				once.Do(func() {
					atomic.StoreInt32(&hasError, 1)
					errCh <- fmt.Errorf("WAL error at index %d: %w", item.index, item.err)
				})
				return
			}

			if err := t.applyWALEntry(item.entry); err != nil {
				once.Do(func() {
					atomic.StoreInt32(&hasError, 1)
					errCh <- fmt.Errorf("replay WAL entry failed at index %d: %w", item.index, err)
				})
				return
			}

			if _, err := t.SaveVersion(false); err != nil {
				once.Do(func() {
					atomic.StoreInt32(&hasError, 1)
					errCh <- fmt.Errorf("save version failed at index %d: %w", item.index, err)
				})
				return
			}

			nextProcessIndex++
		}

		select {
		case errCh <- nil:
		default:
		}
	}()

	workerWg.Wait()
	processWg.Wait()

	var err error
	select {
	case err = <-errCh:
	default:
	}
	close(errCh)

	if err != nil {
		return err
	}

	t.UpdateCommitInfo()
	return nil
}

func (t *MultiTree) WriteSnapshot(dir string, wp *pond.WorkerPool) error {
	return t.WriteSnapshotWithContext(context.Background(), dir, wp)
}

func (t *MultiTree) WriteSnapshotWithContext(ctx context.Context, dir string, wp *pond.WorkerPool) error {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	// write the snapshots in parallel and wait all jobs done
	group, _ := wp.GroupContext(context.Background())

	for _, entry := range t.trees {
		tree, name := entry.Tree, entry.Name
		group.Submit(func() error {
			return tree.WriteSnapshotWithContext(ctx, filepath.Join(dir, name))
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	// write commit info
	metadata := MultiTreeMetadata{
		CommitInfo:     &t.lastCommitInfo,
		InitialVersion: int64(t.initialVersion),
	}
	bz, err := metadata.Marshal()
	if err != nil {
		return err
	}
	return WriteFileSync(filepath.Join(dir, MetadataFileName), bz)
}

// WriteFileSync calls `f.Sync` after before closing the file
func WriteFileSync(name string, data []byte) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

func (t *MultiTree) Close() error {
	errs := make([]error, 0, len(t.trees))
	for _, entry := range t.trees {
		errs = append(errs, entry.Close())
	}
	t.trees = nil
	t.treesByName = nil
	t.lastCommitInfo = CommitInfo{}
	return errors.Join(errs...)
}

func nextVersion(v int64, initialVersion uint32) int64 {
	if v == 0 && initialVersion > 1 {
		return int64(initialVersion)
	}
	return v + 1
}

// walIndex converts version to wal index based on initial version
func walIndex(v int64, initialVersion uint32) uint64 {
	if initialVersion > 1 {
		return uint64(v) - uint64(initialVersion) + 1
	}
	return uint64(v)
}

// walVersion converts wal index to version, reverse of walIndex
func walVersion(index uint64, initialVersion uint32) int64 {
	if initialVersion > 1 {
		return int64(index) + int64(initialVersion) - 1
	}
	return int64(index)
}

func readMetadata(dir string) (*MultiTreeMetadata, error) {
	// load commit info
	bz, err := os.ReadFile(filepath.Join(dir, MetadataFileName))
	if err != nil {
		return nil, err
	}
	var metadata MultiTreeMetadata
	if err := metadata.Unmarshal(bz); err != nil {
		return nil, err
	}
	if metadata.CommitInfo.Version > math.MaxUint32 {
		return nil, fmt.Errorf("commit info version overflows uint32: %d", metadata.CommitInfo.Version)
	}
	if metadata.InitialVersion > math.MaxUint32 {
		return nil, fmt.Errorf("initial version overflows uint32: %d", metadata.InitialVersion)
	}

	return &metadata, nil
}
