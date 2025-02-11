package tree

import (
	"context"
	"math"

	"github.com/beevik/etree"
	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/tree/importer"
	"github.com/sdcio/data-server/pkg/types"

	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
)

const (
	KeysIndexSep       = "_"
	DefaultValuesPrio  = int32(math.MaxInt32 - 90)
	DefaultsIntentName = "default"
	RunningValuesPrio  = int32(math.MaxInt32 - 100)
	RunningIntentName  = "running"
	ReplaceValuesPrio  = int32(math.MaxInt32 - 110)
	ReplaceIntentName  = "replace"
)

type EntryImpl struct {
	*sharedEntryAttributes
}

// newEntry constructor for Entries
func newEntry(ctx context.Context, parent Entry, pathElemName string, tc *TreeContext) (*EntryImpl, error) {
	// create a new sharedEntryAttributes instance
	sea, err := newSharedEntryAttributes(ctx, parent, pathElemName, tc)
	if err != nil {
		return nil, err
	}

	newEntry := &EntryImpl{
		sharedEntryAttributes: sea,
	}
	// add the Entry as a child to the parent Entry
	err = parent.addChild(ctx, newEntry)
	return newEntry, err
}

// Entry is the primary Element of the Tree.
type Entry interface {
	// Path returns the Path as PathSlice
	Path() PathSlice
	// PathName returns the last Path element, the name of the Entry
	PathName() string
	// addChild Add a child entry
	addChild(context.Context, Entry) error
	// AddCacheUpdateRecursive Add the given cache.Update to the tree
	AddCacheUpdateRecursive(ctx context.Context, u *cache.Update, flags *UpdateInsertFlags) (Entry, error)
	// StringIndent debug tree struct as indented string slice
	StringIndent(result []string) []string
	// GetHighesPrio return the new cache.Update entried from the tree that are the highes priority.
	// If the onlyNewOrUpdated option is set to true, only the New or Updated entries will be returned
	// It will append to the given list and provide a new pointer to the slice
	GetHighestPrecedence(result LeafVariantSlice, onlyNewOrUpdated bool) LeafVariantSlice
	// getHighestPrecedenceLeafValue returns the highest LeafValue of the Entry at hand
	// will return an error if the Entry is not a Leaf
	getHighestPrecedenceLeafValue(context.Context) (*LeafEntry, error)
	// GetByOwner returns the branches Updates by owner
	GetByOwner(owner string, result []*LeafEntry) []*LeafEntry
	// markOwnerDelete Sets the delete flag on all the LeafEntries belonging to the given owner.
	markOwnerDelete(o string, onlyIntended bool)
	// GetDeletes returns the cache-updates that are not updated, have no lower priority value left and hence should be deleted completely
	GetDeletes(entries []DeleteEntry, aggregatePaths bool) ([]DeleteEntry, error)
	// Walk takes the EntryVisitor and applies it to every Entry in the tree
	Walk(f EntryVisitor) error
	// Validate kicks off validation
	Validate(ctx context.Context, resultChan chan<- *types.ValidationResultEntry, concurrent bool)
	// validateMandatory the Mandatory schema field
	validateMandatory(ctx context.Context, resultChan chan<- *types.ValidationResultEntry)
	// validateMandatoryWithKeys is an internally used function that us called by validateMandatory in case
	// the container has keys defined that need to be skipped before the mandatory attributes can be checked
	validateMandatoryWithKeys(ctx context.Context, level int, attribute string, resultChan chan<- *types.ValidationResultEntry)
	// getHighestPrecedenceValueOfBranch returns the highes Precedence Value (lowest Priority value) of the brach that starts at this Entry
	getHighestPrecedenceValueOfBranch() int32
	// GetSchema returns the *sdcpb.SchemaElem of the Entry
	GetSchema() *sdcpb.SchemaElem
	// IsRoot returns true if the Entry is the root of the tree
	IsRoot() bool
	// FinishInsertionPhase indicates, that the insertion of Entries into the tree is over
	// Hence calculations for e.g. choice/case can be performed.
	FinishInsertionPhase(ctx context.Context)
	// GetParent returns the parent entry
	GetParent() Entry
	// Navigate navigates the tree according to the given path and returns the referenced entry or nil if it does not exist.
	Navigate(ctx context.Context, path []string, isRootPath bool) (Entry, error)
	NavigateSdcpbPath(ctx context.Context, path []*sdcpb.PathElem, isRootPath bool) (Entry, error)
	// NavigateLeafRef follows the leafref and returns the referenced entry
	NavigateLeafRef(ctx context.Context) ([]Entry, error)
	// GetFirstAncestorWithSchema returns the first parent node which has a schema set.
	// if the parent has no schema (is a key element in the tree) it will recurs the call to the parents parent.
	// the level of recursion is indicated via the levelUp attribute
	GetFirstAncestorWithSchema() (ancestor Entry, levelUp int)
	// SdcpbPath returns the sdcpb.Path struct for the Entry
	SdcpbPath() (*sdcpb.Path, error)
	// SdcpbPathInternal is the internal function to calculate the SdcpbPath
	SdcpbPathInternal(spath []string) (*sdcpb.Path, error)
	// GetSchemaKeys checks for the schema of the entry, and returns the defined keys
	GetSchemaKeys() []string
	// GetRootBasedEntryChain returns all the entries starting from the root down to the actual Entry.
	GetRootBasedEntryChain() []Entry
	// GetRoot returns the Trees Root Entry
	GetRoot() Entry
	// remainsToExist indicates if a LeafEntry for this entry will survive the update.
	// Since we add running to the tree, there will always be Entries, that will disappear in the
	// as part of the SetIntent process. We need to consider this, when evaluating e.g. LeafRefs.
	// The returned boolean will in indicate if the value remains existing (true) after the setintent.
	// Or will disappear from device (running) as part of the SetIntent action.
	remainsToExist() bool
	getChildren() map[string]Entry
	FilterChilds(keys map[string]string) ([]Entry, error)
	// ToJson returns the Tree contained structure as JSON
	// use e.g. json.MarshalIndent() on the returned struct
	ToJson(onlyNewOrUpdated bool) (any, error)
	// ToJsonIETF returns the Tree contained structure as JSON_IETF
	// use e.g. json.MarshalIndent() on the returned struct
	ToJsonIETF(onlyNewOrUpdated bool) (any, error)
	// toJsonInternal the internal function that produces JSON and JSON_IETF
	// Not for external usage
	toJsonInternal(onlyNewOrUpdated bool, ietf bool) (j any, err error)
	ToXML(onlyNewOrUpdated bool, honorNamespace bool, operationWithNamespace bool, useOperationRemove bool) (*etree.Document, error)
	toXmlInternal(parent *etree.Element, onlyNewOrUpdated bool, honorNamespace bool, operationWithNamespace bool, useOperationRemove bool) (doAdd bool, err error)
	// ImportConfig allows importing config data received from e.g. the device in different formats (json, xml) to be imported into the tree.
	ImportConfig(ctx context.Context, t importer.ImportConfigAdapter, intentName string, intentPrio int32) error
}

type EntryVisitor func(s *sharedEntryAttributes) error
