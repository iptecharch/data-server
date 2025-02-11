package tree

import (
	"iter"
	"math"
	"sync"

	"github.com/sdcio/data-server/pkg/utils"
)

type LeafVariants struct {
	les      []*LeafEntry
	lesMutex sync.RWMutex
	tc       *TreeContext
}

func newLeafVariants(tc *TreeContext) *LeafVariants {
	return &LeafVariants{
		les: make([]*LeafEntry, 0, 2),
		tc:  tc,
	}
}

func (lv *LeafVariants) Add(le *LeafEntry) {
	if leafVariant := lv.GetByOwner(le.Owner()); leafVariant != nil {
		if leafVariant.EqualSkipPath(le.Update) {
			// it seems like the element was not deleted, so drop the delete flag
			leafVariant.DropDeleteFlag()
		} else {
			// if a leafentry of the same owner exists with different value, mark it for update
			leafVariant.MarkUpdate(le.Update)
		}
	} else {
		lv.lesMutex.Lock()
		defer lv.lesMutex.Unlock()
		// if LeafVaraint with same owner does not exist, add the new entry
		lv.les = append(lv.les, le)
	}
}

// Items iterator for the LeafVariants
func (lv *LeafVariants) Items() iter.Seq[*LeafEntry] {
	return func(yield func(*LeafEntry) bool) {
		lv.lesMutex.RLock()
		defer lv.lesMutex.RUnlock()
		for _, v := range lv.les {
			if !yield(v) {
				return
			}
		}
	}
}

func (lv *LeafVariants) Length() int {
	lv.lesMutex.RLock()
	defer lv.lesMutex.RUnlock()
	return len(lv.les)
}

// ShouldDelete indicates if the entry should be deleted,
// since it is an entry that represents LeafsVariants but non
// of these are still valid.
func (lv *LeafVariants) shouldDelete() bool {
	lv.lesMutex.RLock()
	defer lv.lesMutex.RUnlock()
	// only procede if we have leave variants
	if len(lv.les) == 0 {
		return false
	}

	// if only running exists return false
	if lv.les[0].Update.Owner() == RunningIntentName && len(lv.les) == 1 {
		return false
	}

	// go through all variants
	for _, l := range lv.les {
		// if not running is set and not the owner is running then
		// it should not be deleted
		if !((l.GetDeleteFlag() && !l.GetDeleteOnlyIntendedFlag()) || l.Update.Owner() == RunningIntentName) {
			return false
		}
	}

	return true
}

func (lv *LeafVariants) GetHighestPrecedenceValue() int32 {
	lv.lesMutex.RLock()
	defer lv.lesMutex.RUnlock()
	result := int32(math.MaxInt32)
	for _, e := range lv.les {
		if !e.GetDeleteFlag() && e.Owner() != DefaultsIntentName && e.Update.Priority() < result {
			result = e.Update.Priority()
		}
	}
	return result
}

// GetHighesNewUpdated returns the LeafEntry with the highes priority
// nil if no leaf entry exists.
func (lv *LeafVariants) GetHighestPrecedence(onlyNewOrUpdated bool, includeDefaults bool) *LeafEntry {
	lv.lesMutex.RLock()
	defer lv.lesMutex.RUnlock()
	if len(lv.les) == 0 {
		return nil
	}
	if onlyNewOrUpdated && lv.shouldDelete() {
		return nil
	}

	var highest *LeafEntry
	var secondHighest *LeafEntry
	for _, e := range lv.les {
		// first entry set result to it
		// if it is not marked for deletion
		if highest == nil {
			highest = e
			continue
		}
		// on a result != nil that is then not marked for deletion
		// start comparing priorities and choose the one with the
		// higher prio (lower number)
		if highest.Priority() > e.Priority() {
			secondHighest = highest
			highest = e
		} else {
			// check if the update is at least higher prio (lower number) then the secondHighest
			if secondHighest == nil || secondHighest.Priority() > e.Priority() {
				secondHighest = e
			}
		}
	}

	// do not include defaults loaded at validation time
	if !includeDefaults && highest.Update.Owner() == DefaultsIntentName {
		return nil
	}

	// if it does not matter if the highes update is also
	// New or Updated return it
	if !onlyNewOrUpdated {
		return highest
	}

	// if the highes is not marked for deletion and new or updated (=PrioChanged) return it
	if !highest.GetDeleteFlag() {
		if highest.GetNewFlag() || highest.GetUpdateFlag() || (lv.tc.actualOwner != "" && highest.Update.Owner() == lv.tc.actualOwner && lv.highestNotRunning(highest)) {
			return highest
		}
		return nil
	}
	// otherwise if the secondhighest is not marked for deletion return it
	if secondHighest != nil && !secondHighest.GetDeleteFlag() && secondHighest.Update.Owner() != RunningIntentName {
		return secondHighest
	}

	// otherwise return nil
	return nil
}

func (lv *LeafVariants) highestNotRunning(highest *LeafEntry) bool {
	// if highes is already running or even default, return false
	if highest.Update.Owner() == RunningIntentName {
		return false
	}

	runVal := lv.GetByOwner(RunningIntentName)
	if runVal == nil {
		return true
	}

	// ignore errors, they should not happen :-P I know... should...
	rval, _ := runVal.Value()
	hval, _ := highest.Value()

	return !utils.EqualTypedValues(rval, hval)
}

// GetByOwner returns the entry that is owned by the given owner,
// returns nil if no entry exists.
func (lv *LeafVariants) GetByOwner(owner string) *LeafEntry {
	lv.lesMutex.RLock()
	defer lv.lesMutex.RUnlock()
	for _, e := range lv.les {
		if e.Owner() == owner {
			return e
		}
	}
	return nil
}
