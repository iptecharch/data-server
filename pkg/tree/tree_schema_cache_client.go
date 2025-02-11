package tree

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/sdcio/cache/proto/cachepb"
	"github.com/sdcio/data-server/pkg/cache"
	SchemaClient "github.com/sdcio/data-server/pkg/datastore/clients/schema"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
)

const (
	PATHSEP = "/"
)

type TreeSchemaCacheClient interface {
	// RefreshCaches refresh the running and intended Index cache
	RefreshCaches(ctx context.Context) error

	// CACHE based Functions
	// ReadIntended retrieves the highes priority value from the intended store
	Read(ctx context.Context, opts *cache.Opts, paths [][]string) []*cache.Update

	ReadRunningPath(ctx context.Context, path PathSlice) (*cache.Update, error)
	ReadRunningFull(ctx context.Context) ([]*cache.Update, error)
	GetBranchesHighesPrecedence(ctx context.Context, path []string, filters ...CacheUpdateFilter) int32
	ReadCurrentUpdatesHighestPriorities(ctx context.Context, ccp PathSlices, count uint64) UpdateSlice
	IntendedPathExists(ctx context.Context, path []string) (bool, error)
	ReadUpdatesOwner(ctx context.Context, owner string) UpdateSlice

	// SCHEMA based Functions
	GetSchema(ctx context.Context, path []string) (*sdcpb.GetSchemaResponse, error)
	ToPath(ctx context.Context, path []string) (*sdcpb.Path, error)
}

type TreeSchemaCacheClientImpl struct {
	cc          cache.Client
	schemaIndex *schemaIndex
	datastore   string

	timeout time.Duration

	intendedStoreIndex      map[string]UpdateSlice // contains the keys that the intended store holds in the cache
	intendedStoreIndexMutex sync.RWMutex
	runningStoreIndex       map[string]UpdateSlice // contains the keys of the running config
	runningStoreIndexMutex  sync.RWMutex
}

func NewTreeSchemaCacheClient(datastore string, cc cache.Client, scb SchemaClient.SchemaClientBound) *TreeSchemaCacheClientImpl {
	return &TreeSchemaCacheClientImpl{
		cc:                      cc,
		schemaIndex:             newSchemaIndex(scb),
		datastore:               datastore,
		timeout:                 time.Second * 2,
		intendedStoreIndexMutex: sync.RWMutex{},
		runningStoreIndexMutex:  sync.RWMutex{},
	}
}

func (t *TreeSchemaCacheClientImpl) IntendedPathExists(ctx context.Context, path []string) (bool, error) {
	t.intendedStoreIndexMutex.RLock()
	if t.intendedStoreIndex == nil {
		t.intendedStoreIndexMutex.RUnlock()
		t.RefreshCaches(ctx)
		t.intendedStoreIndexMutex.RLock()
	}
	defer t.intendedStoreIndexMutex.RUnlock()
	_, exists := t.intendedStoreIndex[strings.Join(path, KeysIndexSep)]
	return exists, nil
}

func (c *TreeSchemaCacheClientImpl) Read(ctx context.Context, opts *cache.Opts, paths [][]string) []*cache.Update {
	if opts == nil {
		opts = &cache.Opts{
			PriorityCount: 1,
		}
	}

	return c.cc.Read(ctx, c.datastore, opts, paths, c.timeout)
}

func (c *TreeSchemaCacheClientImpl) RefreshCaches(ctx context.Context) error {

	var err error
	c.runningStoreIndexMutex.Lock()
	c.runningStoreIndex, err = c.readStoreKeysMeta(ctx, cachepb.Store_CONFIG)
	c.runningStoreIndexMutex.Unlock()
	if err != nil {
		return err
	}
	c.intendedStoreIndexMutex.Lock()
	c.intendedStoreIndex, err = c.readStoreKeysMeta(ctx, cachepb.Store_INTENDED)
	c.intendedStoreIndexMutex.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (c *TreeSchemaCacheClientImpl) readStoreKeysMeta(ctx context.Context, store cachepb.Store) (map[string]UpdateSlice, error) {
	entryCh, err := c.cc.GetKeys(ctx, c.datastore, store)
	if err != nil {
		return nil, err
	}

	result := map[string]UpdateSlice{}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case e, ok := <-entryCh:
			if !ok {
				return result, nil
			}
			key := strings.Join(e.GetPath(), KeysIndexSep)
			_, exists := result[key]
			if !exists {
				result[key] = UpdateSlice{}
			}
			result[key] = append(result[key], e)
		}
	}
}

// ToPath local implementation of the ToPath functinality. It takes a string slice that contains schema elements as well as key values.
// Via the help of the schema, the key elemens are being identified and an sdcpb.Path is returned.
func (c *TreeSchemaCacheClientImpl) ToPath(ctx context.Context, path []string) (*sdcpb.Path, error) {
	p := &sdcpb.Path{}
	// iterate through the path slice
	for i := 0; i < len(path); i++ {
		// create a PathElem for the actual index
		newPathElem := &sdcpb.PathElem{Name: path[i]}
		// append the path elem to the path
		p.Elem = append(p.Elem, newPathElem)
		// retrieve the schema
		schema, err := c.schemaIndex.Retrieve(ctx, p)
		if err != nil {
			return nil, err
		}

		// break early if the container itself is defined in the path, not a sub-element
		if len(path) <= i+1 {
			break
		}

		// if it is a container with keys
		if schemaKeys := schema.GetSchema().GetContainer().GetKeys(); schemaKeys != nil {
			// add key map
			newPathElem.Key = make(map[string]string, len(schemaKeys))
			// adding the keys with the value from path[i], which is the key value
			for _, k := range schemaKeys {
				i++
				newPathElem.Key[k.Name] = path[i]
			}
		}
	}
	return p, nil
}

// GetSchema retrieves the given schema element from the schema-server.
// relies on TreeSchemaCacheClientImpl.retrieveSchema(...) to source the internal lookup index (cache) of schemas
func (c *TreeSchemaCacheClientImpl) GetSchema(ctx context.Context, path []string) (*sdcpb.GetSchemaResponse, error) {
	// convert the []string path into sdcpb.path for schema retrieval
	sdcpbPath, err := c.ToPath(ctx, path)
	if err != nil {
		return nil, err
	}

	return c.schemaIndex.Retrieve(ctx, sdcpbPath)
}

func (c *TreeSchemaCacheClientImpl) GetBranchesHighesPrecedence(ctx context.Context, path []string, filters ...CacheUpdateFilter) int32 {
	result := int32(math.MaxInt32)
	pathKey := strings.Join(path, KeysIndexSep)
	c.intendedStoreIndexMutex.RLock()
	if c.intendedStoreIndex == nil {
		c.intendedStoreIndexMutex.RUnlock()
		c.RefreshCaches(ctx)
		c.intendedStoreIndexMutex.RLock()
	}
	defer c.intendedStoreIndexMutex.RUnlock()

	// TODO: Improve this, since it is probably an expensive operation
	for key, entries := range c.intendedStoreIndex {
		if strings.HasPrefix(key, pathKey) {
			if prio := entries.GetLowestPriorityValue(filters); prio < result {
				result = prio
			}
		}
	}
	return result
}

func (c *TreeSchemaCacheClientImpl) ReadCurrentUpdatesHighestPriorities(ctx context.Context, ccp PathSlices, count uint64) UpdateSlice {
	return c.Read(ctx, &cache.Opts{
		Store:         cachepb.Store_INTENDED,
		PriorityCount: count,
	}, ccp.ToStringSlice())
}

func (c *TreeSchemaCacheClientImpl) ReadUpdatesOwner(ctx context.Context, owner string) UpdateSlice {

	ownerPaths := c.getPathsOfOwner(ctx, owner)

	return c.Read(ctx, &cache.Opts{
		Store: cachepb.Store_INTENDED,
		Owner: owner,
	}, ownerPaths.paths.ToStringSlice())
}

func (c *TreeSchemaCacheClientImpl) getPathsOfOwner(ctx context.Context, owner string) *PathSet {
	if c.intendedStoreIndex == nil {
		c.RefreshCaches(ctx)
	}

	p := NewPathSet()
	for _, keyMeta := range c.intendedStoreIndex {
		for _, k := range keyMeta {
			if k.Owner() == owner {
				// if the key is not yet listed in the keys slice, add it otherwise skip
				p.AddPath(k.GetPath())
			}
		}
	}
	return p
}

// ReadRunning reads the value from running if the value does not exist, nil is returned
func (c *TreeSchemaCacheClientImpl) ReadRunningPath(ctx context.Context, path PathSlice) (*cache.Update, error) {
	c.runningStoreIndexMutex.RLock()
	if c.runningStoreIndex == nil {
		c.runningStoreIndexMutex.RUnlock()
		c.RefreshCaches(ctx)
		c.runningStoreIndexMutex.RLock()
	}
	defer c.runningStoreIndexMutex.RUnlock()
	// check if the value exists in running
	_, exists := c.runningStoreIndex[strings.Join(path, KeysIndexSep)]
	if !exists {
		return nil, nil
	}

	updates := c.Read(ctx, &cache.Opts{
		Store:         cachepb.Store_CONFIG,
		PriorityCount: 1,
	}, [][]string{path})

	return updates[0], nil
}

// ReadRunning reads the value from running if the value does not exist, nil is returned
func (c *TreeSchemaCacheClientImpl) ReadRunningFull(ctx context.Context) ([]*cache.Update, error) {
	updates := c.Read(ctx, &cache.Opts{
		Store: cachepb.Store_CONFIG,
	}, [][]string{{}})

	return updates, nil
}
