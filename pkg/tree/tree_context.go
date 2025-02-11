package tree

import (
	"fmt"
)

type TreeContext struct {
	root                  Entry // the trees root element
	treeSchemaCacheClient TreeSchemaCacheClient
	actualOwner           string
}

func NewTreeContext(tscc TreeSchemaCacheClient, actualOwner string) *TreeContext {
	return &TreeContext{
		treeSchemaCacheClient: tscc,
		actualOwner:           actualOwner,
	}
}

// deepCopy root is required to be set manually
func (t *TreeContext) deepCopy() *TreeContext {
	return &TreeContext{
		treeSchemaCacheClient: t.treeSchemaCacheClient,
	}
}

func (t *TreeContext) GetTreeSchemaCacheClient() TreeSchemaCacheClient {
	return t.treeSchemaCacheClient
}

func (t *TreeContext) SetRoot(e Entry) error {
	if t.root != nil {
		return fmt.Errorf("trying to set treecontexts root, although it is already set")
	}
	t.root = e
	return nil
}

func (t *TreeContext) GetActualOwner() string {
	return t.actualOwner
}

func (t *TreeContext) SetActualOwner(owner string) {
	t.actualOwner = owner
}
