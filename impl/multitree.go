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

	zeroCopy  bool
	cacheSize int

	trees          []NamedTree    // always ordered by tree name
	treesByName    map[string]int // index of the trees by name
	lastCommitInfo CommitInfo

	// the initial metadata loaded from disk snapshot
	metadata MultiTreeMetadata
}

func NewEmptyMultiTree(initialVersion uint32, cacheSize int) *MultiTree {
	return &MultiTree{
		initialVersion: initialVersion,
		treesByName:    make(map[string]int),
		zeroCopy:       true,
		cacheSize:      cacheSize,
	}
}

func LoadMultiTree(dir string, zeroCopy bool, cacheSize int) (*MultiTree, error) {
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
	var infos []StoreInfo
	for _, entry := range t.trees {
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

	if endIndex < firstIndex {
		return fmt.Errorf("target index %d is pruned", endIndex)
	}

	if endIndex > lastIndex {
		return fmt.Errorf("target index %d is in the future, latest index: %d", endIndex, lastIndex)
	}

	return t.processByRingBuffer(wal, firstIndex, endIndex)
	//	var (
	//		readWorkers = 20                         // concurrent reading wal
	//		bufferSize  = lastIndex - firstIndex + 1 // chan size
	//	)
	//
	//	type walItem struct {
	//		index uint64
	//		entry WALEntry
	//		err   error
	//	}
	//
	//	walChan := make(chan walItem, bufferSize)
	//
	//	var wg sync.WaitGroup
	//	for w := 0; w < readWorkers; w++ {
	//		wg.Add(1)
	//		go func(workerId int) {
	//			defer wg.Done()
	//
	//			for i := firstIndex + uint64(workerId); i <= endIndex; i += uint64(readWorkers) {
	//				bz, err := wal.Read(i)
	//				if err != nil {
	//					walChan <- walItem{index: i, err: fmt.Errorf("read wal log failed at index %d: %w", i, err)}
	//					return
	//				}
	//
	//				var entry WALEntry
	//				if err := entry.Unmarshal(bz); err != nil {
	//					walChan <- walItem{index: i, err: fmt.Errorf("unmarshal wal log failed at index %d: %w", i, err)}
	//					return
	//				}
	//
	//				walChan <- walItem{index: i, entry: entry}
	//			}
	//		}(w)
	//	}
	//
	//	go func() {
	//		wg.Wait()
	//		close(walChan)
	//	}()
	//
	//	// main goroutine to process WAL
	//	var lastProcessedIndex uint64
	//	expectedIndex := firstIndex
	//	pendingItems := make(map[uint64]walItem) // catch wal with larger id
	//
	//LOOP:
	//	for {
	//		select {
	//		case item, ok := <-walChan:
	//			if !ok {
	//				if expectedIndex <= endIndex {
	//					return fmt.Errorf("incomplete WAL processing, expected up to %d, got to %d", endIndex, lastProcessedIndex)
	//				}
	//				break LOOP
	//			}
	//
	//			if item.err != nil {
	//				return item.err
	//			}
	//
	//			// process wal entry
	//			if item.index == expectedIndex {
	//				if err := t.applyWALEntry(item.entry); err != nil {
	//					return fmt.Errorf("replay wal entry failed at index %d: %w", item.index, err)
	//				}
	//				if _, err := t.SaveVersion(false); err != nil {
	//					return fmt.Errorf("replay change set failed at index %d: %w", item.index, err)
	//				}
	//				lastProcessedIndex = item.index
	//				expectedIndex++
	//
	//			} else {
	//				pendingItems[item.index] = item
	//			}
	//			// find expectedIndex from pending catch
	//			for {
	//				if nextItem, ok := pendingItems[expectedIndex]; ok {
	//					if err := t.applyWALEntry(nextItem.entry); err != nil {
	//						return fmt.Errorf("replay wal entry failed at index %d: %w", nextItem.index, err)
	//					}
	//					if _, err := t.SaveVersion(false); err != nil {
	//						return fmt.Errorf("replay change set failed at index %d: %w", nextItem.index, err)
	//					}
	//					delete(pendingItems, expectedIndex)
	//					lastProcessedIndex = expectedIndex
	//					expectedIndex++
	//				} else {
	//					break
	//				}
	//			}
	//		default:
	//			if lastProcessedIndex == endIndex {
	//				break LOOP
	//			}
	//		}
	//	}
	//
	//	t.UpdateCommitInfo()
	//	return nil
}

func (t *MultiTree) processByRingBuffer(wal *wal.Log, firstIndex, endIndex uint64) error {
	type walItem struct {
		index uint64
		entry WALEntry
		err   error
	}

	readWorkers := 10
	bufferSize := readWorkers * 2
	if bufferSize > int(endIndex-firstIndex+1) {
		bufferSize = int(endIndex - firstIndex + 1)
	}

	ringBuffer := make([]walItem, bufferSize)
	for i := range ringBuffer {
		ringBuffer[i].index = math.MaxUint64 // math.MaxUint64 means can be used or reused
	}

	var (
		mu               sync.Mutex
		readPos          = 0
		processPos       = 0
		nextReadIndex    = firstIndex
		nextProcessIndex = firstIndex
		done             = false
		errCh            = make(chan error, 1)
	)

	var workerWg sync.WaitGroup

	// readWorkers read WALs concurrently
	for w := 0; w < readWorkers; w++ {
		workerWg.Add(1)
		go func(workerId int) {
			defer workerWg.Done()
			for {
				mu.Lock()
				if nextReadIndex > endIndex {
					mu.Unlock()
					return
				}
				readIndex := nextReadIndex
				nextReadIndex++
				mu.Unlock()

				bz, err := wal.Read(readIndex)
				var entry WALEntry
				if err == nil {
					err = entry.Unmarshal(bz)
				}

				item := walItem{
					index: readIndex,
					entry: entry,
					err:   err,
				}

				// insert ringBuffer
				for {
					mu.Lock()
					if ringBuffer[readPos].index == math.MaxUint64 {
						ringBuffer[readPos] = item
						readPos = (readPos + 1) % bufferSize
						mu.Unlock()
						break
					}
					mu.Unlock()
					time.Sleep(time.Millisecond)
				}
			}
		}(w)
	}

	// process WAL
	var processWg sync.WaitGroup
	processWg.Add(1)
	go func() {
		defer processWg.Done()
		for {
			mu.Lock()
			if done && nextProcessIndex > endIndex {
				mu.Unlock()
				errCh <- nil
				return
			}
			item := ringBuffer[processPos]
			if item.index == nextProcessIndex {
				ringBuffer[processPos] = walItem{}
				processPos = (processPos + 1) % bufferSize
				mu.Unlock()

				if item.err != nil {
					errCh <- item.err
					return
				}
				if err := t.applyWALEntry(item.entry); err != nil {
					errCh <- fmt.Errorf("replay wal entry failed at index %d: %w", item.index, err)
					return
				}
				if _, err := t.SaveVersion(false); err != nil {
					errCh <- fmt.Errorf("replay change set failed at index %d: %w", item.index, err)
					return
				}

				nextProcessIndex++
			} else {
				mu.Unlock()
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// wait all read worker finish
	workerWg.Wait()
	mu.Lock()
	done = true
	mu.Unlock()

	// wait finish processing WAL
	processWg.Wait()

	close(errCh)
	err := <-errCh
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
