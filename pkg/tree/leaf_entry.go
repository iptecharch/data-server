package tree

import (
	"fmt"
	"sync"

	"github.com/sdcio/data-server/pkg/cache"
)

// LeafEntry stores the *cache.Update along with additional attributes.
// These Attributes indicate if the entry is to be deleted / added (new) or updated.
type LeafEntry struct {
	*cache.Update
	parentEntry        Entry
	IsNew              bool
	Delete             bool
	DeleteOnlyIntended bool
	IsUpdated          bool

	mu sync.RWMutex
}

func (l *LeafEntry) GetEntry() Entry {
	return l.parentEntry
}

// MarkUpdate indicate that the entry is an Updated value
func (l *LeafEntry) MarkUpdate(u *cache.Update) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// set the new value
	l.Update = u
	// set the update flag
	l.IsUpdated = true
	// reset the delete flag
	l.Delete = false
	l.IsNew = false
}

func (l *LeafEntry) MarkNew() {
	l.mu.Lock()
	defer l.mu.Unlock()
	// set the update flag
	l.IsUpdated = false
	// reset the delete flag
	l.Delete = false
	l.DeleteOnlyIntended = false
	l.IsNew = true
}

func (l *LeafEntry) GetDeleteFlag() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.Delete
}

func (l *LeafEntry) GetDeleteOnlyIntendedFlag() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.DeleteOnlyIntended
}

func (l *LeafEntry) GetUpdateFlag() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.IsUpdated
}

func (l *LeafEntry) GetNewFlag() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.IsNew
}

func (l *LeafEntry) DropDeleteFlag() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Delete = false
	l.DeleteOnlyIntended = false
}

// MarkDelete indicate that the entry is to be deleted
func (l *LeafEntry) MarkDelete(onlyIntended bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Delete = true
	if onlyIntended {
		l.DeleteOnlyIntended = true
	}
	l.IsUpdated = false
	l.IsNew = false
}

func (l *LeafEntry) GetRootBasedEntryChain() []Entry {
	return l.parentEntry.GetRootBasedEntryChain()
}

// String returns a string representation of the LeafEntry
func (l *LeafEntry) String() string {
	tv, err := l.Value()
	var v string
	if err != nil {
		v = err.Error()
	} else {
		v = tv.String()
	}
	return fmt.Sprintf("Owner: %s, Priority: %d, Value: %s, New: %t, Delete: %t, Update: %t, DeleteIntendedOnly: %t", l.Owner(), l.Priority(), v, l.GetNewFlag(), l.GetDeleteFlag(), l.GetUpdateFlag(), l.GetDeleteOnlyIntendedFlag())
}

// NewLeafEntry constructor for a new LeafEntry
func NewLeafEntry(c *cache.Update, flags *UpdateInsertFlags, parent Entry) *LeafEntry {
	le := &LeafEntry{
		parentEntry: parent,
		Update:      c,
	}
	flags.Apply(le)
	return le

}
