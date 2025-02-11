package tree

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/tree/importer"
	"github.com/sdcio/data-server/pkg/types"
	"github.com/sdcio/data-server/pkg/utils"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// sharedEntryAttributes contains the attributes shared by Entry and RootEntry
type sharedEntryAttributes struct {
	// parent entry, nil for the root Entry
	parent Entry
	// pathElemName the path elements name the entry represents
	pathElemName string
	// childs mutual exclusive with LeafVariants
	childs      *childMap
	childsMutex sync.RWMutex
	// leafVariants mutual exclusive with Childs
	// If Entry is a leaf it can hold multiple leafVariants
	leafVariants *LeafVariants
	// schema the schema element for this entry
	schema      *sdcpb.SchemaElem
	schemaMutex sync.RWMutex

	choicesResolvers choiceCasesResolvers

	treeContext *TreeContext

	// state cache
	remains      *bool
	remainsMutex sync.Mutex
}

func (s *sharedEntryAttributes) deepCopy(tc *TreeContext, parent Entry) (*sharedEntryAttributes, error) {
	result := &sharedEntryAttributes{
		parent:           parent,
		pathElemName:     s.pathElemName,
		childs:           newChildMap(),
		leafVariants:     newLeafVariants(tc),
		schema:           s.schema,
		treeContext:      tc,
		choicesResolvers: s.choicesResolvers.deepCopy(),
	}
	return result, nil
}

type childMap struct {
	c  map[string]Entry
	mu sync.RWMutex
}

func newChildMap() *childMap {
	return &childMap{
		c: map[string]Entry{},
	}
}

func (c *childMap) Add(e Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.c[e.PathName()] = e
}

func (c *childMap) GetEntry(s string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, exists := c.c[s]
	return e, exists
}

func (c *childMap) GetAll() map[string]Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := map[string]Entry{}
	for k, v := range c.c {
		result[k] = v
	}
	return result
}

func (c *childMap) GetKeys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]string, 0, c.Length())
	for k := range c.c {
		result = append(result, k)
	}
	return result
}

func (c *childMap) Length() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.c)
}

func newSharedEntryAttributes(ctx context.Context, parent Entry, pathElemName string, tc *TreeContext) (*sharedEntryAttributes, error) {
	s := &sharedEntryAttributes{
		parent:       parent,
		pathElemName: pathElemName,
		childs:       newChildMap(),
		leafVariants: newLeafVariants(tc),
		treeContext:  tc,
	}

	// populate the schema
	err := s.populateSchema(ctx)
	if err != nil {
		return nil, err
	}

	// initialize the choice case resolvers with the schema information
	s.initChoiceCasesResolvers()

	return s, nil
}

func (s *sharedEntryAttributes) GetRoot() Entry {
	if s.parent == nil {
		return s
	}
	return s.parent.GetRoot()
}

func (s *sharedEntryAttributes) populateSchema(ctx context.Context) error {
	s.schemaMutex.Lock()
	defer s.schemaMutex.Unlock()

	getSchema := true

	// on the root element we cannot query the parent schema.
	// hence skip this part if IsRoot
	if !s.IsRoot() {

		// we can and should skip schema retrieval if we have a
		// terminal value that is a key value.
		// to check for that, we query the parent for the schema even multiple levels up
		// because we can have multiple keys. we remember the number of levels we moved up
		// and if that is within the len of keys, we're still in a key level, and need to skip
		// querying the schema. Otherwise we need to query the schema.
		ancesterschema, levelUp := s.GetFirstAncestorWithSchema()

		// check the found schema
		switch schem := ancesterschema.GetSchema().GetSchema().(type) {
		case *sdcpb.SchemaElem_Container:
			// if it is a container and level up is less or equal the levelUp count
			// this means, we are on a level this is for sure still a key level in the tree
			if len(schem.Container.GetKeys()) >= levelUp {
				getSchema = false
				break
			}
		}
	}

	if getSchema {
		// trieve if the getSchema var is still true
		schemaResp, err := s.treeContext.treeSchemaCacheClient.GetSchema(ctx, s.Path())
		if err != nil {
			return err
		}
		s.schema = schemaResp.GetSchema()
	}

	return nil
}

// GetSchema return the schema fiels of the Entry
func (s *sharedEntryAttributes) GetSchema() *sdcpb.SchemaElem {
	s.schemaMutex.RLock()
	defer s.schemaMutex.RUnlock()
	return s.schema
}

// GetChildren returns the children Map of the Entry
func (s *sharedEntryAttributes) getChildren() map[string]Entry {
	return s.childs.GetAll()
}

// FilterChilds returns the child entries (skipping the key entries in the tree) that
// match the given keys. The keys do not need to match all levels of keys, in which case the
// key level is considered a wildcard match (*)
func (s *sharedEntryAttributes) FilterChilds(keys map[string]string) ([]Entry, error) {
	if s.schema == nil {
		return nil, fmt.Errorf("error non schema level %s", s.Path().String())
	}

	result := []Entry{}
	// init the processEntries with s
	processEntries := []Entry{s}

	// retrieve the schema keys
	schemaKeys := s.GetSchemaKeys()
	// sort the keys, such that they appear in the order that they
	// are inserted in the tree
	sort.Strings(schemaKeys)
	// iterate through the keys, resolving the key levels
	for _, key := range schemaKeys {
		keyVal, exist := keys[key]
		// if the key exists in the input map meaning has a filter value
		// associated, the childs map is filtered for that value
		if exist {
			// therefor we need to go through the processEntries List
			// and collect all the matching childs
			for _, entry := range processEntries {
				childs := entry.getChildren()
				matchEntry, childExists := childs[keyVal]
				// so if such child, that matches the given filter value exists, we append it to the results
				if childExists {
					result = append(result, matchEntry)
				}
			}
		} else {
			// this is basically the wildcard case, so go through all childs and add them
			result = []Entry{}
			for _, entry := range processEntries {
				childs := entry.getChildren()
				for _, v := range childs {
					// hence we add all the existing childs to the result list
					result = append(result, v)
				}
			}
		}
		// prepare for the next iteration
		processEntries = result
	}
	return result, nil
}

// GetParent returns the parent entry
func (s *sharedEntryAttributes) GetParent() Entry {
	return s.parent
}

// IsRoot returns true if the element has no parent elements, hence is the root of the tree
func (s *sharedEntryAttributes) IsRoot() bool {
	return s.parent == nil
}

// GetLevel returns the level / depth position of this element in the tree
func (s *sharedEntryAttributes) GetLevel() int {
	return len(s.Path())
}

// Walk takes the EntryVisitor and applies it to every Entry in the tree
func (s *sharedEntryAttributes) Walk(f EntryVisitor) error {

	// TODO: COME UP WITH SOME CLEVER CONCURRENCY

	// execute the function locally
	err := f(s)
	if err != nil {
		return err
	}

	// trigger the execution on all childs
	for _, c := range s.childs.GetAll() {
		err := c.Walk(f)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSchemaKeys checks for the schema of the entry, and returns the defined keys
func (s *sharedEntryAttributes) GetSchemaKeys() []string {
	if s.schema != nil {
		// if the schema is a container schema, we need to process the aggregation logic
		if contschema := s.schema.GetContainer(); contschema != nil {
			// if the level equals the amount of keys defined, we're at the right level, where the
			// actual elements start (not in a key level within the tree)
			var keys []string
			for _, k := range contschema.GetKeys() {
				keys = append(keys, k.Name)
			}
			return keys
		}
	}
	return nil
}

// getAggregatedDeletes is called on levels that have no schema attached, meaning key schemas.
// here we might delete the whole branch of the tree, if all key elements are being deleted
// if not, we continue with regular deltes
func (s *sharedEntryAttributes) getAggregatedDeletes(deletes []DeleteEntry, aggregatePaths bool) ([]DeleteEntry, error) {
	var err error
	// we take a look into the level(s) up
	// trying to get the schema
	ancestor, level := s.GetFirstAncestorWithSchema()

	// check if the first schema on the path upwards (parents)
	// has keys defined (meaning is a contianer with keys)
	keys := ancestor.GetSchemaKeys()

	// if keys exist and we're on the last level of the keys, validate
	// if aggregation can happen
	if len(keys) > 0 && level == len(keys) {
		doAggregateDelete := true
		// check the keys for deletion
		for _, n := range keys {
			c, exists := s.childs.GetEntry(n)
			// these keys should aways exist, so for now we do not catch the non existing key case
			if exists && c.remainsToExist() {
				// if not all the keys are marked for deletion, we need to revert to regular deletion
				doAggregateDelete = false
				break
			}
		}
		// if aggregate delet is possible do it
		if doAggregateDelete {
			// by adding the key path to the deletes
			deletes = append(deletes, s)
		} else {
			// otherwise continue with deletion on the childs.
			for _, c := range s.childs.GetAll() {
				deletes, err = c.GetDeletes(deletes, aggregatePaths)
				if err != nil {
					return nil, err
				}
			}
		}
		return deletes, nil
	}
	return s.getRegularDeletes(deletes, aggregatePaths)
}

func (s *sharedEntryAttributes) remainsToExist() bool {
	// see if we have the value cached
	s.remainsMutex.Lock()
	defer s.remainsMutex.Unlock()
	if s.remains != nil {
		return *s.remains
	}

	leafVariantResult := s.leafVariants.Length() > 0 && !s.leafVariants.shouldDelete()

	// handle containers
	childsRemain := false
	for _, c := range s.filterActiveChoiceCaseChilds() {
		childsRemain = c.remainsToExist()
		if childsRemain {
			break
		}
	}
	activeChoiceCase := false
	// only needs to be checked if it still looks like there
	// it is to be deleted
	if !childsRemain {
		activeChoiceCase = s.choicesResolvers.remainsToExist()
	}

	// assumption is, that if the entry exists, there is at least a running value available.
	remains := leafVariantResult || childsRemain || activeChoiceCase
	// populate the state cache
	s.remains = &remains

	return remains
}

// getRegularDeletes performs deletion calculation on elements that have a schema attached.
func (s *sharedEntryAttributes) getRegularDeletes(deletes []DeleteEntry, aggregate bool) ([]DeleteEntry, error) {
	var err error
	// if entry is a container type, check the keys, to be able to
	// issue a delte for the whole branch at once via keys
	switch s.schema.GetSchema().(type) {
	case *sdcpb.SchemaElem_Container:

		// deletes for child elements (choice cases) that newly became inactive.
		for _, v := range s.choicesResolvers {
			oldBestCaseName := v.getOldBestCaseName()
			newBestCaseName := v.getBestCaseName()
			// so if we have an old and a new best cases (not "") and the names are different,
			// all the old to the deletion list
			if oldBestCaseName != "" && newBestCaseName != "" && oldBestCaseName != newBestCaseName {
				// try fetching the case from the childs
				oldBestCaseEntry, exists := s.childs.GetEntry(oldBestCaseName)
				if exists {
					deletes = append(deletes, oldBestCaseEntry)
				} else {
					// it might be that the child is not loaded into the tree, but just considered from the treecontext cache for the choice/case resolution
					// if so, we create and return the DeleteEntryImpl struct
					path, err := s.SdcpbPath()
					if err != nil {
						return nil, err
					}
					deletes = append(deletes, NewDeleteEntryImpl(path, append(s.Path(), oldBestCaseName)))
				}
			}
		}
	}

	if !s.remainsToExist() && !s.IsRoot() && len(s.GetSchemaKeys()) == 0 {
		return append(deletes, s), nil
	}

	for _, e := range s.childs.GetAll() {
		deletes, err = e.GetDeletes(deletes, aggregate)
		if err != nil {
			return nil, err
		}
	}
	return deletes, nil
}

// GetDeletes calculate the deletes that need to be send to the device.
func (s *sharedEntryAttributes) GetDeletes(deletes []DeleteEntry, aggregatePaths bool) ([]DeleteEntry, error) {

	// if the actual level has no schema assigned we're on a key level
	// element. Hence we try deletion via aggregation
	if s.schema == nil && aggregatePaths {
		return s.getAggregatedDeletes(deletes, aggregatePaths)
	}

	// else perform regular deletion
	return s.getRegularDeletes(deletes, aggregatePaths)
}

// GetAncestorSchema returns the schema of the parent node if the schema is set.
// if the parent has no schema (is a key element in the tree) it will recurs the call to the parents parent.
// the level of recursion is indicated via the levelUp attribute
func (s *sharedEntryAttributes) GetFirstAncestorWithSchema() (Entry, int) {
	// if root node is reached
	if s.parent == nil {
		return nil, 0
	}
	// check if the parent has a schema
	if s.parent.GetSchema() != nil {
		// if so return it with level 1
		return s.parent, 1
	}
	// direct parent does not have a schema, recurse the call
	schema, level := s.parent.GetFirstAncestorWithSchema()
	// increase the level returned by the parent to
	// reflect this entry as a level and return
	return schema, level + 1
}

// GetByOwner returns all the LeafEntries that belong to a certain owner.
func (s *sharedEntryAttributes) GetByOwner(owner string, result []*LeafEntry) []*LeafEntry {
	lv := s.leafVariants.GetByOwner(owner)
	if lv != nil {
		result = append(result, lv)
	}

	// continue with childs
	for _, c := range s.childs.GetAll() {
		result = c.GetByOwner(owner, result)
	}
	return result
}

// Path returns the root based path of the Entry
func (s *sharedEntryAttributes) Path() PathSlice {
	// special handling for root node
	if s.parent == nil {
		return PathSlice{}
	}
	return append(s.parent.Path(), s.pathElemName)
}

// PathName returns the name of the Entry
func (s *sharedEntryAttributes) PathName() string {
	return s.pathElemName
}

// String returns a string representation of the Entry
func (s *sharedEntryAttributes) String() string {
	return strings.Join(s.parent.Path(), "/")
}

// addChild add an entry to the list of child entries for the entry.
func (s *sharedEntryAttributes) addChild(ctx context.Context, e Entry) error {
	// make sure Entry should not only hold LeafEntries
	if s.leafVariants.Length() > 0 {
		// An exception are presence containers
		_, is_container := s.schema.Schema.(*sdcpb.SchemaElem_Container)
		if !is_container && !s.schema.GetContainer().IsPresence {
			return fmt.Errorf("cannot add child to %s since it holds Leafs", s)
		}
	}
	// check the path of child is a subpath of s
	if !slices.Equal(s.Path(), e.Path()[:len(e.Path())-1]) {
		return fmt.Errorf("adding Child with diverging path, parent: %s, child: %s", s, strings.Join(e.Path()[:len(e.Path())-1], "/"))
	}
	s.childs.Add(e)
	return nil
}

func (s *sharedEntryAttributes) NavigateSdcpbPath(ctx context.Context, pathElems []*sdcpb.PathElem, isRootPath bool) (Entry, error) {
	var err error
	if len(pathElems) == 0 {
		return s, nil
	}

	if isRootPath {
		return s.GetRoot().NavigateSdcpbPath(ctx, pathElems, false)
	}

	switch pathElems[0].Name {
	case ".":
		s.NavigateSdcpbPath(ctx, pathElems[1:], false)
	case "..":
		var entry Entry
		entry = s.parent
		// we need to skip key levels in the tree
		// if the next path element is again .. we need to skip key values that are present in the tree
		// If it is a sub-entry instead, we need to stay in the brach that is defined by the key values
		// hence only delegate the call to the parent

		if len(pathElems) > 1 && pathElems[1].Name == ".." {
			entry, _ = s.GetFirstAncestorWithSchema()
		}
		return entry.NavigateSdcpbPath(ctx, pathElems[1:], false)
	default:
		e, exists := s.filterActiveChoiceCaseChilds()[pathElems[0].Name]
		if !exists {
			e, err = s.tryLoading(ctx, []string{pathElems[0].Name})
			if err != nil {
				return nil, err
			}
			if e != nil {
				exists = true
			}
		}

		if !exists {
			pth := &sdcpb.Path{Elem: pathElems}
			e, err = s.tryLoadingDefault(ctx, utils.ToStrings(pth, false, false))
			if err != nil {
				pathStr := utils.ToXPath(pth, false)
				return nil, fmt.Errorf("navigating tree, reached %v but child %v does not exist, trying to load defaults yielded %v", s.Path(), pathStr, err)
			}
			return e, nil
		}

		for _, v := range pathElems[0].Key {
			e, err = e.Navigate(ctx, []string{v}, false)
			if err != nil {
				return nil, err
			}
		}

		return e.NavigateSdcpbPath(ctx, pathElems[1:], false)
	}

	return nil, fmt.Errorf("navigating tree, reached %v but child %v does not exist", s.Path(), pathElems)
}

func (s *sharedEntryAttributes) tryLoadingDefault(ctx context.Context, path []string) (Entry, error) {

	schema, err := s.treeContext.treeSchemaCacheClient.GetSchema(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("error trying to load defaults for %s: %v", strings.Join(path, "->"), err)
	}

	var tv *sdcpb.TypedValue

	switch schem := schema.GetSchema().GetSchema().(type) {
	case *sdcpb.SchemaElem_Field:
		defaultVal := schem.Field.GetDefault()
		if defaultVal == "" {
			return nil, fmt.Errorf("error no default defined for %s", strings.Join(path, " -> "))
		}
		tv, err = utils.Convert(defaultVal, schem.Field.GetType())
		if err != nil {
			return nil, err
		}
	case *sdcpb.SchemaElem_Leaflist:
		listDefaults := schem.Leaflist.GetDefaults()
		tvlist := make([]*sdcpb.TypedValue, 0, len(listDefaults))
		for _, dv := range schem.Leaflist.GetDefaults() {
			tvelem, err := utils.Convert(dv, schem.Leaflist.GetType())
			if err != nil {
				return nil, fmt.Errorf("error converting default to typed value for %s, type: %s ; value: %s; err: %v", strings.Join(path, " -> "), schem.Leaflist.GetType().GetTypeName(), dv, err)
			}
			tvlist = append(tvlist, tvelem)
		}
		tv = &sdcpb.TypedValue{
			Value: &sdcpb.TypedValue_LeaflistVal{
				LeaflistVal: &sdcpb.ScalarArray{
					Element: tvlist,
				},
			},
		}
	default:
		return nil, fmt.Errorf("error no defaults defined for schema path: %s", strings.Join(path, "->"))
	}

	// convert value to []byte for cache insertion
	val, err := proto.Marshal(tv)
	if err != nil {
		return nil, err
	}

	upd := cache.NewUpdate(path, val, DefaultValuesPrio, DefaultsIntentName, 0)

	flags := NewUpdateInsertFlags()

	result, err := s.AddCacheUpdateRecursive(ctx, upd, flags)
	if err != nil {
		return nil, fmt.Errorf("failed adding default value for %s to tree; %v", strings.Join(path, "/"), err)
	}

	return result, nil
}

// Navigate move through the tree, returns the Entry that is present under the given path
func (s *sharedEntryAttributes) Navigate(ctx context.Context, path []string, isRootPath bool) (Entry, error) {
	if len(path) == 0 {
		return s, nil
	}

	if isRootPath {
		return s.GetRoot().Navigate(ctx, path, false)
	}
	var err error
	switch path[0] {
	case ".":
		return s.Navigate(ctx, path[1:], false)
	case "..":
		return s.parent.Navigate(ctx, path[1:], false)
	default:
		e, exists := s.filterActiveChoiceCaseChilds()[path[0]]
		if !exists {
			e, _ = s.tryLoading(ctx, append(s.Path(), path...))
			if e != nil {
				exists = true
			}
		}
		if !exists {
			e, err = s.tryLoadingDefault(ctx, append(s.Path(), path...))
			if err != nil {
				return nil, fmt.Errorf("navigating tree, reached %v but child %v does not exist, trying to load defaults yielded %v", s.Path(), path, err)
			}
			return e, nil
		}
		return e.Navigate(ctx, path[1:], false)
	}
}

func (s *sharedEntryAttributes) tryLoading(ctx context.Context, path []string) (Entry, error) {
	upd, err := s.treeContext.GetTreeSchemaCacheClient().ReadRunningPath(ctx, append(s.Path(), path...))
	if err != nil {
		return nil, err
	}
	if upd == nil {
		return nil, fmt.Errorf("reached %v but child %s does not exist", s.Path(), path[0])
	}
	flags := NewUpdateInsertFlags()

	_, err = s.treeContext.root.AddCacheUpdateRecursive(ctx, upd, flags)
	if err != nil {
		return nil, err
	}

	e, _ := s.childs.GetEntry(path[0])
	return e, nil
}

// GetHighestPrecedence goes through the whole branch and returns the new and updated cache.Updates.
// These are the updated that will be send to the device.
func (s *sharedEntryAttributes) GetHighestPrecedence(result LeafVariantSlice, onlyNewOrUpdated bool) LeafVariantSlice {
	// get the highes precedence LeafeVariant and add it to the list
	lv := s.leafVariants.GetHighestPrecedence(onlyNewOrUpdated, false)
	if lv != nil {
		result = append(result, lv)
	}

	// continue with childs. Childs are part of choices, process only the "active" (highes precedence) childs
	for _, c := range s.filterActiveChoiceCaseChilds() {
		result = c.GetHighestPrecedence(result, onlyNewOrUpdated)
	}
	return result
}

func (s *sharedEntryAttributes) getHighestPrecedenceLeafValue(ctx context.Context) (*LeafEntry, error) {
	for _, x := range []string{"existing", "default"} {
		lv := s.leafVariants.GetHighestPrecedence(false, true)
		if lv != nil {
			return lv, nil
		}
		if x != "default" {
			_, err := s.tryLoadingDefault(ctx, s.Path())
			if err != nil {
				return nil, err
			}
		}
	}
	return nil, fmt.Errorf("error no value present for %s", s.Path())
}

func (s *sharedEntryAttributes) GetRootBasedEntryChain() []Entry {
	if s.IsRoot() {
		return []Entry{}
	}
	return append(s.parent.GetRootBasedEntryChain(), s)
}

// getHighestPrecedenceValueOfBranch goes through all the child branches to find the highes
// precedence value (lowest priority value) for the entire branch and returns it.
func (s *sharedEntryAttributes) getHighestPrecedenceValueOfBranch() int32 {
	result := int32(math.MaxInt32)
	for _, e := range s.childs.GetAll() {
		if val := e.getHighestPrecedenceValueOfBranch(); val < result {
			result = val
		}
	}
	if val := s.leafVariants.GetHighestPrecedenceValue(); val < result {
		result = val
	}

	return result
}

// Validate is the highlevel function to perform validation.
// it will multiplex all the different Validations that need to happen
func (s *sharedEntryAttributes) Validate(ctx context.Context, resultChan chan<- *types.ValidationResultEntry, concurrent bool) {

	// recurse the call to the child elements
	wg := sync.WaitGroup{}
	defer wg.Wait()
	for _, c := range s.filterActiveChoiceCaseChilds() {
		wg.Add(1)
		valFunc := func(x Entry) { // HINT: for Must-Statement debugging, remove "go " such that the debugger is triggered one after the other
			x.Validate(ctx, resultChan, concurrent)
			wg.Done()
		}
		if concurrent {
			go valFunc(c)
		} else {
			valFunc(c)
		}
	}

	// validate the mandatory statement on this entry
	if s.remainsToExist() {
		s.validateMandatory(ctx, resultChan)
		s.validateLeafRefs(ctx, resultChan)
		s.validateLeafListMinMaxAttributes(resultChan)
		s.validatePattern(resultChan)
		s.validateMustStatements(ctx, resultChan)
		s.validateLength(resultChan)
		s.validateRange(resultChan)
	}
}

func (s *sharedEntryAttributes) validateRange(resultCHan chan<- *types.ValidationResultEntry) {

	// lv := s.leafVariants.GetHighestPrecedence(false)
	// if lv == nil {
	// 	return
	// }

	// tv, err := lv.Value()
	// if err != nil {
	// 	errchan <- fmt.Errorf("failed reading value from %s LeafVariant %v: %w", s.Path(), lv, err)
	// 	return
	// }

	// if schema := s.schema.GetField(); schema != nil {
	// 	switch schema.GetType().TypeName {
	// 	case "uint8", "uint16", "uint32", "uint64":
	// 		urange := &utils.NewUrnges()
	// 	}

	// 	for _, rng := range schema.GetType().Range {

	// 	}
	// }

}

// validateLeafListMinMaxAttributes validates the Min-, and Max-Elements attribute of the Entry if it is a Leaflists.
func (s *sharedEntryAttributes) validateLeafListMinMaxAttributes(resultChan chan<- *types.ValidationResultEntry) {
	if schema := s.schema.GetLeaflist(); schema != nil {
		if schema.MinElements > 0 {
			if lv := s.leafVariants.GetHighestPrecedence(false, true); lv != nil {
				tv, err := lv.Update.Value()
				if err != nil {
					resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("validating LeafList Min Attribute: %v", err), types.ValidationResultEntryTypeError)
				}
				if val := tv.GetLeaflistVal(); val != nil {
					// check minelements if set
					if schema.MinElements > 0 && len(val.GetElement()) < int(schema.GetMinElements()) {
						resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("leaflist %s defines %d min-elements but only %d elements are present", s.Path().String(), schema.MinElements, len(val.GetElement())), types.ValidationResultEntryTypeError)
					}
					// check maxelements if set
					if len(val.GetElement()) > int(schema.GetMaxElements()) {
						resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("leaflist %s defines %d max-elements but %d elements are present", s.Path().String(), schema.GetMaxElements(), len(val.GetElement())), types.ValidationResultEntryTypeError)
					}
				}
			}
		}
	}
}

func (s *sharedEntryAttributes) validateLength(resultChan chan<- *types.ValidationResultEntry) {
	if schema := s.schema.GetField(); schema != nil {

		if len(schema.GetType().Length) == 0 {
			return
		}

		lv := s.leafVariants.GetHighestPrecedence(false, true)
		if lv == nil {
			return
		}

		tv, err := lv.Value()
		if err != nil {
			resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("failed reading value from %s LeafVariant %v: %w", s.Path(), lv, err), types.ValidationResultEntryTypeError)
			return
		}
		value := tv.GetStringVal()
		actualLength := utf8.RuneCountInString(value)

		for _, lengthDef := range schema.GetType().Length {
			if lengthDef.Min.Value <= uint64(actualLength) && uint64(actualLength) <= lengthDef.Max.Value {
				return
			}
		}
		lenghts := []string{}
		for _, lengthDef := range schema.GetType().Length {
			lenghts = append(lenghts, fmt.Sprintf("%d..%d", lengthDef.Min.Value, lengthDef.Max.Value))
		}
		resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("error length of Path: %s, Value: %s not within allowed length %s", s.Path(), value, strings.Join(lenghts, ", ")), types.ValidationResultEntryTypeError)
	}
}

func (s *sharedEntryAttributes) validatePattern(resultChan chan<- *types.ValidationResultEntry) {
	if schema := s.schema.GetField(); schema != nil {
		if len(schema.Type.Patterns) == 0 {
			return
		}
		lv := s.leafVariants.GetHighestPrecedence(false, true)
		tv, err := lv.Update.Value()
		if err != nil {
			resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("failed reading value from %s LeafVariant %v: %w", s.Path(), lv, err), types.ValidationResultEntryTypeError)
			return
		}
		value := tv.GetStringVal()
		for _, pattern := range schema.Type.Patterns {
			if p := pattern.GetPattern(); p != "" {
				matched, err := regexp.MatchString(p, value)
				if err != nil {
					resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("failed compiling regex %s defined for %s", p, s.Path()), types.ValidationResultEntryTypeError)
					continue
				}
				if (!matched && !pattern.Inverted) || (pattern.GetInverted() && matched) {
					resultChan <- types.NewValidationResultEntry(lv.Owner(), fmt.Errorf("value %s of %s does not match regex %s (inverted: %t)", value, s.Path(), p, pattern.GetInverted()), types.ValidationResultEntryTypeError)
				}
			}
		}
	}
}

func (s *sharedEntryAttributes) ImportConfig(ctx context.Context, t importer.ImportConfigAdapter, intentName string, intentPrio int32) error {
	var err error

	updateInsertFlags := NewUpdateInsertFlags()
	if intentName != RunningIntentName {
		updateInsertFlags.SetNewFlag()
	}

	switch x := s.schema.GetSchema().(type) {
	case *sdcpb.SchemaElem_Container, nil:
		switch {
		case len(s.schema.GetContainer().GetKeys()) > 0:

			var exists bool
			var actualEntry Entry = s
			var keyChild Entry
			for _, keySchema := range s.schema.GetContainer().GetKeys() {

				keyElemName := keySchema.Name

				keyTransf := t.GetElement(keyElemName)
				if keyTransf == nil {
					return fmt.Errorf("unable to find key attribute %s under %s", keyElemName, s.Path())
				}
				keyElemValue := keyTransf.GetKeyValue()
				// if the child does not exist, create it
				if keyChild, exists = actualEntry.getChildren()[keyElemValue]; !exists {
					keyChild, err = newEntry(ctx, actualEntry, keyElemValue, s.treeContext)
					if err != nil {
						return err
					}
				}
				actualEntry = keyChild
			}
			err = actualEntry.ImportConfig(ctx, t, intentName, intentPrio)
			if err != nil {
				return err
			}
		default:
			if len(t.GetElements()) == 0 {
				// it might be a presence container
				schem := s.schema.GetContainer()
				if schem == nil {
					return nil
				}
				if schem.IsPresence {
					tv := &sdcpb.TypedValue{Value: &sdcpb.TypedValue_EmptyVal{EmptyVal: &emptypb.Empty{}}}
					tvVal, err := proto.Marshal(tv)
					if err != nil {
						return err
					}
					upd := cache.NewUpdate(s.Path(), tvVal, intentPrio, intentName, 0)
					s.leafVariants.Add(NewLeafEntry(upd, updateInsertFlags, s))
				}
			}

			for _, elem := range t.GetElements() {
				var child Entry
				var exists bool

				// if the child does not exist, create it
				if child, exists = s.getChildren()[elem.GetName()]; !exists {
					child, err = newEntry(ctx, s, elem.GetName(), s.treeContext)
					if err != nil {
						return fmt.Errorf("error trying to insert %s at path %v: %w", elem.GetName(), s.Path(), err)
					}
					err = s.addChild(ctx, child)
					if err != nil {
						return err
					}
				}
				err = child.ImportConfig(ctx, elem, intentName, intentPrio)
				if err != nil {
					return err
				}
			}
		}
	case *sdcpb.SchemaElem_Field:
		// // if it is as leafref we need to figure out the type of the references field.
		fieldType := x.Field.GetType()
		// if x.Field.GetType().Type == "leafref" {
		// 	s.treeContext.treeSchemaCacheClient.GetSchema(ctx,)
		// }

		tv, err := t.GetTVValue(fieldType)
		if err != nil {
			return err
		}
		tvVal, err := proto.Marshal(tv)
		if err != nil {
			return err
		}
		upd := cache.NewUpdate(s.Path(), tvVal, intentPrio, intentName, 0)

		s.leafVariants.Add(NewLeafEntry(upd, updateInsertFlags, s))

	case *sdcpb.SchemaElem_Leaflist:
		var scalarArr *sdcpb.ScalarArray
		mustAdd := false
		le := s.leafVariants.GetByOwner(intentName)
		if le != nil {
			llvTv, err := le.Update.Value()
			if err != nil {
				return err
			}

			scalarArr = llvTv.GetLeaflistVal()
		} else {
			le = NewLeafEntry(nil, updateInsertFlags, s)
			mustAdd = true
			scalarArr = &sdcpb.ScalarArray{Element: []*sdcpb.TypedValue{}}
		}

		tv, err := t.GetTVValue(x.Leaflist.GetType())
		if err != nil {
			return err
		}
		scalarArr.Element = append(scalarArr.Element, tv)

		tvVal, err := proto.Marshal(&sdcpb.TypedValue{Value: &sdcpb.TypedValue_LeaflistVal{LeaflistVal: scalarArr}})
		if err != nil {
			return err
		}

		le.Update = cache.NewUpdate(s.Path(), tvVal, intentPrio, intentName, 0)
		if mustAdd {
			s.leafVariants.Add(le)
		}
	}
	return nil
}

// validateMandatory validates that all the mandatory attributes,
// defined by the schema are present either in the tree or in the index.
func (s *sharedEntryAttributes) validateMandatory(ctx context.Context, resultChan chan<- *types.ValidationResultEntry) {
	if s.schema != nil {
		switch s.schema.GetSchema().(type) {
		case *sdcpb.SchemaElem_Container:
			for _, c := range s.schema.GetContainer().GetMandatoryChildrenConfig() {
				s.validateMandatoryWithKeys(ctx, len(s.GetSchema().GetContainer().GetKeys()), c.Name, resultChan)
			}
		}
	}
}

func (s *sharedEntryAttributes) validateMandatoryWithKeys(ctx context.Context, level int, attribute string, resultChan chan<- *types.ValidationResultEntry) {
	if level == 0 {
		// first check if the mandatory value is set via the intent, e.g. part of the tree already
		v, existsInTree := s.filterActiveChoiceCaseChilds()[attribute]

		// if not the path exists in the tree and is not to be deleted, then lookup in the paths index of the store
		// and see if such path exists, if not raise the error
		if !(existsInTree && v.remainsToExist()) {
			exists, err := s.treeContext.treeSchemaCacheClient.IntendedPathExists(ctx, append(s.Path(), attribute))
			if err != nil {
				resultChan <- types.NewValidationResultEntry(s.leafVariants.GetHighestPrecedence(false, false).Owner(), fmt.Errorf("error validating mandatory childs %s: %v", s.Path(), err), types.ValidationResultEntryTypeError)
			}
			if !exists {
				resultChan <- types.NewValidationResultEntry(s.leafVariants.GetHighestPrecedence(false, false).Owner(), fmt.Errorf("error mandatory child %s does not exist, path: %s", attribute, s.Path()), types.ValidationResultEntryTypeError)
			}
		}
		return
	}

	for _, c := range s.filterActiveChoiceCaseChilds() {
		c.validateMandatoryWithKeys(ctx, level-1, attribute, resultChan)
	}

}

// initChoiceCasesResolvers Choices and their cases are defined in the schema.
// We need the information on which choices exist and what the below cases are.
// Therefore the choiceCasesResolvers are initialized with the information.
// At a later stage, when the insertion of values into the tree is completed,
// the choiceCasesResolvers will get the priority values per branch and use these to
// calculate the active case.
func (s *sharedEntryAttributes) initChoiceCasesResolvers() {
	if s.schema == nil {
		return
	}

	// extract container schema
	var ci *sdcpb.ChoiceInfo
	switch s.schema.GetSchema().(type) {
	case *sdcpb.SchemaElem_Container:
		ci = s.schema.GetContainer().GetChoiceInfo()
	}

	// create a new choiceCasesResolvers struct
	choicesResolvers := choiceCasesResolvers{}

	// iterate through choices defined in schema
	for choiceName, choice := range ci.GetChoice() {
		// add the choice to the choiceCasesResolvers
		actualResolver := choicesResolvers.AddChoice(choiceName)
		// iterate through cases
		for caseName, choiceCase := range choice.GetCase() {
			// add cases and their depending elements / attributes to the case
			actualResolver.AddCase(caseName, choiceCase.GetElements())
		}
	}
	// set the resolver in the sharedEntryAttributes
	s.choicesResolvers = choicesResolvers
}

// FinishInsertionPhase certain values that are costly to calculate but used multiple times
// will be calculated and stored for later use. However therefore the insertion phase into the
// tree needs to be over. Calling this function indicated the end of the phase and thereby triggers the calculation
func (s *sharedEntryAttributes) FinishInsertionPhase(ctx context.Context) {

	// populate the ChoiceCaseResolvers to determine the active case
	s.populateChoiceCaseResolvers(ctx)

	// recurse the call to all (active) entries within the tree.
	// Thereby already using the choiceCaseResolver via filterActiveChoiceCaseChilds()
	for _, child := range s.filterActiveChoiceCaseChilds() {
		child.FinishInsertionPhase(ctx)
	}

	// reset state
	s.remains = nil
}

// populateChoiceCaseResolvers iterates through the ChoiceCaseResolvers,
// retrieving the childs that nake up all the cases. per these childs
// (branches in the tree), the Highes precedence is being retrieved from the
// caches index (old intent content) as well as from the tree (new intent content).
// the choiceResolver is fed with the resulting values and thereby ready to be queried
// in a later stage (filterActiveChoiceCaseChilds()).
func (s *sharedEntryAttributes) populateChoiceCaseResolvers(ctx context.Context) {
	if s.schema == nil {
		return
	}
	// if choice/cases exist, process it
	for _, choiceResolver := range s.choicesResolvers {
		for _, elem := range choiceResolver.GetElementNames() {
			isNew := false
			var val2 *int32
			// Query the Index, stored in the treeContext for the per branch highes precedence
			v := s.treeContext.GetTreeSchemaCacheClient().GetBranchesHighesPrecedence(ctx, append(s.Path(), elem), CacheUpdateFilterExcludeOwner(s.treeContext.GetActualOwner()))

			child, childExists := s.childs.GetEntry(elem)
			// set the value from the tree as well
			if childExists {
				x := child.getHighestPrecedenceValueOfBranch()
				val2 = &x
			}

			if val2 != nil && v >= *val2 {
				v = *val2
				isNew = true
			}
			choiceResolver.SetValue(elem, v, isNew)
		}
	}
}

// filterActiveChoiceCaseChilds returns the list of child elements. In case the Entry is
// a container with a / multiple choices, the list of childs is filtered to only return the
// cases that have the highest precedence.
func (s *sharedEntryAttributes) filterActiveChoiceCaseChilds() map[string]Entry {
	if s.schema == nil {
		return s.childs.GetAll()
	}

	skipAttributesList := s.choicesResolvers.GetSkipElements()
	result := map[string]Entry{}
	// optimization option: sort the slices and forward in parallel, lifts extra burden that the contains call holds.
	for childName, child := range s.childs.GetAll() {
		if slices.Contains(skipAttributesList, childName) {
			continue
		}
		result[childName] = child
	}
	return result
}

// StringIndent returns the sharedEntryAttributes in its string representation
// The string is intented according to the nesting level in the yang model
func (s *sharedEntryAttributes) StringIndent(result []string) []string {
	result = append(result, strings.Repeat("  ", s.GetLevel())+s.pathElemName)

	// ranging over children and LeafVariants
	// then should be mutual exclusive, either a node has children or LeafVariants

	// range over children
	for _, c := range s.childs.GetAll() {
		result = c.StringIndent(result)
	}
	// range over LeafVariants
	for l := range s.leafVariants.Items() {
		result = append(result, fmt.Sprintf("%s -> %s", strings.Repeat("  ", s.GetLevel()), l.String()))
	}
	return result
}

// markOwnerDelete Sets the delete flag on all the LeafEntries belonging to the given owner.
func (s *sharedEntryAttributes) markOwnerDelete(o string, onlyIntended bool) {
	lvEntry := s.leafVariants.GetByOwner(o)
	// if an entry for the given user exists, mark it for deletion
	if lvEntry != nil {
		lvEntry.MarkDelete(onlyIntended)
	}
	// recurse into childs
	for _, child := range s.childs.GetAll() {
		child.markOwnerDelete(o, onlyIntended)
	}
}

// SdcpbPath returns the sdcpb.Path, with its elements and keys based on the local schema
func (s *sharedEntryAttributes) SdcpbPath() (*sdcpb.Path, error) {
	return s.SdcpbPathInternal(s.Path())
}

// sdcpbPathInternal is the internale recursive function to calculate and the sdcpb.Path,
// with its elements and keys based on the local schema
func (s *sharedEntryAttributes) SdcpbPathInternal(spath []string) (*sdcpb.Path, error) {
	// if we moved all the way up to the root
	// we create a new sdcpb.Path and return it.
	if s.IsRoot() {
		return &sdcpb.Path{
			Elem: []*sdcpb.PathElem{},
		}, nil
	}
	// if not root, we take the parents result of this
	// recursive call and add our (s) information to the Path.
	p, err := s.parent.SdcpbPathInternal(spath)
	if err != nil {
		return nil, err
	}

	// the element has a schema attached, so we need to add a new element to the
	// path.
	if s.schema != nil {
		switch s.schema.GetSchema().(type) {
		case *sdcpb.SchemaElem_Container, *sdcpb.SchemaElem_Field, *sdcpb.SchemaElem_Leaflist:
			pe := &sdcpb.PathElem{
				Name: s.pathElemName,
				Key:  map[string]string{},
			}
			p.Elem = append(p.Elem, pe)
		}

		// the element does not have a schema attached, hence we need to add a key to
		// the last element that was pushed to the pathElems
	} else {
		name, err := s.getKeyName()
		if err != nil {
			return nil, err
		}
		p.Elem[len(p.Elem)-1].Key[name] = s.PathName()
	}
	return p, err
}

// getKeyName checks if s is a key level element in the tree, if not an error is throw
// if it is a key level element, the name of the key is determined via the ancestor schemas
func (s *sharedEntryAttributes) getKeyName() (string, error) {
	// if the entry has a schema, it cannot be a Key attribute
	if s.schema != nil {
		return "", fmt.Errorf("error %s is a schema element, can only get KeyNames for key element", strings.Join(s.Path(), " "))
	}

	// get ancestro schema
	ancestorWithSchema, levelUp := s.GetFirstAncestorWithSchema()

	// only Contaieners have keys, so check for that
	switch sch := ancestorWithSchema.GetSchema().GetSchema().(type) {
	case *sdcpb.SchemaElem_Container:
		// return the name of the levelUp-1 key
		return sch.Container.GetKeys()[levelUp-1].Name, nil
	}

	// we probably called the function on a LeafList or LeafEntry which is not a valid call to be made.
	return "", fmt.Errorf("error LeafList and Field should not have keys %s", strings.Join(s.Path(), " "))
}

// AddCacheUpdateRecursive recursively adds the given cache.Update to the tree. Thereby creating all the entries along the path.
// if the entries along th path already exist, the existing entries are called to add the Update.
func (s *sharedEntryAttributes) AddCacheUpdateRecursive(ctx context.Context, c *cache.Update, flags *UpdateInsertFlags) (Entry, error) {
	idx := 0
	// if it is the root node, index remains == 0
	if s.parent != nil {
		idx = s.GetLevel()
	}
	// end of path reached, add LeafEntry
	// continue with recursive add otherwise
	if idx == len(c.GetPath()) {
		// delegate update handling to leafVariants
		s.leafVariants.Add(NewLeafEntry(c, flags, s))
		return s, nil
	}

	var e Entry
	var err error
	var exists bool
	// if child does not exist, create Entry
	if e, exists = s.childs.GetEntry(c.GetPath()[idx]); !exists {
		e, err = newEntry(ctx, s, c.GetPath()[idx], s.treeContext)
		if err != nil {
			return nil, err
		}
	}
	return e.AddCacheUpdateRecursive(ctx, c, flags)
}

type UpdateInsertFlags struct {
	new          bool
	delete       bool
	onlyIntended bool
}

// NewUpdateInsertFlags returns a new *UpdateInsertFlags instance
// with all values set to false, so not new, and not marked for deletion
func NewUpdateInsertFlags() *UpdateInsertFlags {
	return &UpdateInsertFlags{}
}

func (f *UpdateInsertFlags) SetDeleteFlag() {
	f.delete = true
	f.new = false
}

func (f *UpdateInsertFlags) SetDeleteOnlyUpdatedFlag() {
	f.delete = true
	f.onlyIntended = true
	f.new = false
}

func (f *UpdateInsertFlags) SetNewFlag() {
	f.new = true
	f.delete = false
	f.onlyIntended = false
}

func (f *UpdateInsertFlags) GetDeleteFlag() bool {
	return f.delete
}

func (f *UpdateInsertFlags) GetDeleteOnlyIntendedFlag() bool {
	return f.onlyIntended
}

func (f *UpdateInsertFlags) GetNewFlag() bool {
	return f.new
}

func (f *UpdateInsertFlags) Apply(le *LeafEntry) {
	if f.delete {
		le.MarkDelete(f.onlyIntended)
		return
	}
	if f.new {
		le.MarkNew()
		return
	}
}
