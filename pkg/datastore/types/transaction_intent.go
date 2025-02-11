package types

import (
	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/tree"
)

type TransactionIntent struct {
	name string
	// updates is nil if the intent did not exist.
	updates      tree.UpdateSlice
	delete       bool
	onlyIntended bool
	priority     int32
}

func NewTransactionIntent(name string, priority int32) *TransactionIntent {
	return &TransactionIntent{
		name: name,
	}
}

func (ti *TransactionIntent) GetName() string {
	return ti.name
}

func (ti *TransactionIntent) GetPriority() int32 {
	return ti.priority
}

func (ti *TransactionIntent) AddUpdates(u tree.UpdateSlice) {
	ti.updates = append(ti.updates, u...)
}

func (ti *TransactionIntent) GetUpdates() tree.UpdateSlice {
	return ti.updates
}

func (ti *TransactionIntent) GetOnlyIntended() bool {
	return ti.onlyIntended
}

func (ti *TransactionIntent) SetDeleteFlag() {
	ti.delete = true
}
func (ti *TransactionIntent) SetDeleteOnlyIntendedFlag() {
	ti.delete = true
	ti.onlyIntended = true
}

func (ti *TransactionIntent) GetPathSet() *tree.PathSet {
	return ti.updates.ToPathSet()
}

func (ti *TransactionIntent) AddUpdate(u *cache.Update) {
	ti.updates = append(ti.updates, u)
}
