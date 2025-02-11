// Copyright 2024 Nokia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datastore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sdcio/cache/proto/cachepb"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	status "google.golang.org/grpc/status"

	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/config"
	"github.com/sdcio/data-server/pkg/datastore/clients"
	"github.com/sdcio/data-server/pkg/datastore/target"
	"github.com/sdcio/data-server/pkg/datastore/types"
	"github.com/sdcio/data-server/pkg/schema"
	"github.com/sdcio/data-server/pkg/tree"
	"github.com/sdcio/data-server/pkg/utils"
)

type Datastore struct {
	// datastore config
	config *config.DatastoreConfig

	cacheClient cache.Client

	// SBI target of this datastore
	sbi target.Target

	// schema server client
	// schemaClient sdcpb.SchemaServerClient
	schemaClient schema.Client

	// client, bound to schema and version on the schema side and to datastore name on the cache side
	// do not use directly use getValidationClient()
	_validationClientBound clients.ValidationClient

	// sync channel, to be passed to the SBI Sync method
	synCh chan *target.SyncUpdate

	// stop cancel func
	cfn context.CancelFunc

	// keeps track of clients watching deviation updates
	m                *sync.RWMutex
	deviationClients map[string]sdcpb.DataServer_WatchDeviationsServer

	// per path intent deviations (no unhandled)
	md                       *sync.RWMutex
	currentIntentsDeviations map[string][]*sdcpb.WatchDeviationResponse

	// datastore mutex locks the whole datasore for further set operations
	dmutex *sync.Mutex

	// TransactionManager
	transactionManager *types.TransactionManager

	treeCacheSchemaClient tree.TreeSchemaCacheClient
}

// New creates a new datastore, its schema server client and initializes the SBI target
// func New(c *config.DatastoreConfig, schemaServer *config.RemoteSchemaServer) *Datastore {
func New(ctx context.Context, c *config.DatastoreConfig, scc schema.Client, cc cache.Client, opts ...grpc.DialOption) *Datastore {
	ds := &Datastore{
		config:                   c,
		schemaClient:             scc,
		cacheClient:              cc,
		m:                        &sync.RWMutex{},
		md:                       &sync.RWMutex{},
		dmutex:                   &sync.Mutex{},
		deviationClients:         make(map[string]sdcpb.DataServer_WatchDeviationsServer),
		currentIntentsDeviations: make(map[string][]*sdcpb.WatchDeviationResponse),
	}
	ds.transactionManager = types.NewTransactionManager(NewDatastoreRollbackAdapter(ds))
	ds.treeCacheSchemaClient = tree.NewTreeSchemaCacheClient(ds.Name(), ds.cacheClient, ds.getValidationClient())

	if c.Sync != nil {
		ds.synCh = make(chan *target.SyncUpdate, c.Sync.Buffer)
	}
	ctx, cancel := context.WithCancel(ctx)
	ds.cfn = cancel

	// create cache instance if needed
	// this is a blocking  call
	ds.initCache(ctx)

	go func() {
		// init sbi, this is a blocking call
		err := ds.connectSBI(ctx, opts...)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			log.Errorf("failed to create SBI for target %s: %v", ds.Config().Name, err)
			return
		}
		// start syncing goroutine
		if c.Sync != nil {
			go ds.Sync(ctx)
		}
		// start deviation goroutine
		ds.DeviationMgr(ctx)
	}()
	return ds
}

func (d *Datastore) initCache(ctx context.Context) {
START:
	ok, err := d.cacheClient.Exists(ctx, d.config.Name)
	if err != nil {
		log.Errorf("failed to check cache instance %s, %s", d.config.Name, err)
		time.Sleep(time.Second)
		goto START
	}
	if ok {
		log.Debugf("cache %q already exists", d.config.Name)
		return
	}

	log.Infof("cache %s does not exist, creating it", d.config.Name)
CREATE:
	err = d.cacheClient.Create(ctx, d.config.Name, false, false)
	if err != nil {
		log.Errorf("failed to create cache %s: %v", d.config.Name, err)
		time.Sleep(time.Second)
		goto CREATE
	}
}

func (d *Datastore) connectSBI(ctx context.Context, opts ...grpc.DialOption) error {
	var err error
	d.sbi, err = target.New(ctx, d.config.Name, d.config.SBI, d.getValidationClient(), opts...)
	if err == nil {
		return nil
	}

	log.Errorf("failed to create DS %s target: %v", d.config.Name, err)
	ticker := time.NewTicker(d.config.SBI.ConnectRetry)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			d.sbi, err = target.New(ctx, d.config.Name, d.config.SBI, d.getValidationClient(), opts...)
			if err != nil {
				log.Errorf("failed to create DS %s target: %v", d.config.Name, err)
				continue
			}
			return nil
		}
	}
}

func (d *Datastore) Name() string {
	return d.config.Name
}

func (d *Datastore) Schema() *config.SchemaConfig {
	return d.config.Schema
}

func (d *Datastore) Config() *config.DatastoreConfig {
	return d.config
}

func (d *Datastore) Candidates(ctx context.Context) ([]*sdcpb.DataStore, error) {
	cand, err := d.cacheClient.GetCandidates(ctx, d.Name())
	if err != nil {
		return nil, err
	}
	rsp := make([]*sdcpb.DataStore, 0, len(cand))
	for _, cd := range cand {
		rsp = append(rsp, &sdcpb.DataStore{
			Type:     sdcpb.Type_CANDIDATE,
			Name:     cd.CandidateName,
			Owner:    cd.Owner,
			Priority: cd.Priority,
		})
	}
	return rsp, nil
}

func (d *Datastore) Discard(ctx context.Context, req *sdcpb.DiscardRequest) error {
	return d.cacheClient.Discard(ctx, req.GetName(), req.Datastore.GetName())
}

func (d *Datastore) CreateCandidate(ctx context.Context, ds *sdcpb.DataStore) error {
	if ds.GetPriority() < 0 {
		return fmt.Errorf("invalid priority value must be >0")
	}
	if ds.GetPriority() <= 0 {
		ds.Priority = 1
	}
	if ds.GetOwner() == "" {
		ds.Owner = DefaultOwner
	}
	return d.cacheClient.CreateCandidate(ctx, d.Name(), ds.GetName(), ds.GetOwner(), ds.GetPriority())
}

func (d *Datastore) DeleteCandidate(ctx context.Context, name string) error {
	return d.cacheClient.DeleteCandidate(ctx, d.Name(), name)
}

func (d *Datastore) ConnectionState() *target.TargetStatus {
	if d.sbi == nil {
		return target.NewTargetStatus(target.TargetStatusNotConnected)
	}
	return d.sbi.Status()
}

func (d *Datastore) Stop() error {
	if d == nil {
		return nil
	}
	d.cfn()
	if d.sbi == nil {
		return nil
	}
	err := d.sbi.Close()
	if err != nil {
		log.Errorf("datastore %s failed to close the target connection: %v", d.Name(), err)
	}
	return nil
}

func (d *Datastore) DeleteCache(ctx context.Context) error {
	return d.cacheClient.Delete(ctx, d.config.Name)
}

func (d *Datastore) Sync(ctx context.Context) {
	// this semaphore controls the number of concurrent writes to the cache
	sem := semaphore.NewWeighted(d.config.Sync.WriteWorkers)
	go d.sbi.Sync(ctx,
		d.config.Sync,
		d.synCh,
	)

	var err error
	var pruneID string
MAIN:
	for {
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				log.Errorf("datastore %s sync stopped: %v", d.Name(), ctx.Err())
			}
			return
		case syncup := <-d.synCh:
			if syncup.Start {
				log.Debugf("%s: sync start", d.Name())
				for {
					pruneID, err = d.cacheClient.CreatePruneID(ctx, d.Name(), syncup.Force)
					if err != nil {
						log.Errorf("datastore %s failed to create prune ID: %v", d.Name(), err)
						time.Sleep(time.Second)
						continue // retry
					}
					continue MAIN
				}
			}
			if syncup.End && pruneID != "" {
				log.Debugf("%s: sync end", d.Name())
				for {
					err = d.cacheClient.ApplyPrune(ctx, d.Name(), pruneID)
					if err != nil {
						log.Errorf("datastore %s failed to prune cache after update: %v", d.Name(), err)
						time.Sleep(time.Second)
						continue // retry
					}
					break
				}
				log.Debugf("%s: sync resetting pruneID", d.Name())
				pruneID = ""
				continue // MAIN FOR loop
			}
			// a regular notification
			log.Debugf("%s: sync acquire semaphore", d.Name())
			err = sem.Acquire(ctx, 1)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Infof("datastore %s sync stopped", d.config.Name)
					return
				}
				log.Errorf("failed to acquire semaphore: %v", err)
				continue
			}
			log.Debugf("%s: sync acquired semaphore", d.Name())
			go d.storeSyncMsg(ctx, syncup, sem)
		}
	}
}

func isState(r *sdcpb.GetSchemaResponse) bool {
	switch r := r.Schema.Schema.(type) {
	case *sdcpb.SchemaElem_Container:
		return r.Container.IsState
	case *sdcpb.SchemaElem_Field:
		return r.Field.IsState
	case *sdcpb.SchemaElem_Leaflist:
		return r.Leaflist.IsState
	}
	return false
}

func (d *Datastore) storeSyncMsg(ctx context.Context, syncup *target.SyncUpdate, sem *semaphore.Weighted) {
	defer sem.Release(1)

	converter := utils.NewConverter(d.getValidationClient())

	cNotification, err := converter.ConvertNotificationTypedValues(ctx, syncup.Update)
	if err != nil {
		log.Errorf("failed to convert notification typedValue: %v", err)
		return
	}

	upds := NewSdcpbUpdateDedup()
	for _, x := range cNotification.GetUpdate() {
		addUpds, err := converter.ExpandUpdateKeysAsLeaf(ctx, x)
		if err != nil {
			continue
		}
		upds.AddUpdate(x)
		upds.AddUpdates(addUpds)
	}
	cNotification.Update = upds.Updates()

	for _, x := range cNotification.GetUpdate() {
		fmt.Printf("%s\n", x.String())
	}

	for _, del := range cNotification.GetDelete() {
		store := cachepb.Store_CONFIG
		if d.config.Sync != nil && d.config.Sync.Validate {
			scRsp, err := d.getSchema(ctx, del)
			if err != nil {
				log.Errorf("datastore %s failed to get schema for delete path %v: %v", d.config.Name, del, err)
				continue
			}
			if isState(scRsp) {
				store = cachepb.Store_STATE
			}
		}
		delPath := utils.ToStrings(del, false, false)
		rctx, cancel := context.WithTimeout(ctx, time.Minute) // TODO:
		defer cancel()
		err = d.cacheClient.Modify(rctx, d.Config().Name,
			&cache.Opts{
				Store: store,
			},
			[][]string{delPath}, nil)
		if err != nil {
			log.Errorf("datastore %s failed to delete path %v: %v", d.config.Name, delPath, err)
		}
	}

	for _, upd := range cNotification.GetUpdate() {
		store := cachepb.Store_CONFIG
		if d.config.Sync != nil && d.config.Sync.Validate {
			scRsp, err := d.getSchema(ctx, upd.GetPath())
			if err != nil {
				log.Errorf("datastore %s failed to get schema for update path %v: %v", d.config.Name, upd.GetPath(), err)
				continue
			}
			if isState(scRsp) {
				store = cachepb.Store_STATE
			}
		}
		// TODO:[KR] convert update typedValue if needed
		cUpd, err := d.cacheClient.NewUpdate(upd)
		if err != nil {
			log.Errorf("datastore %s failed to create update from %v: %v", d.config.Name, upd, err)
			continue
		}

		rctx, cancel := context.WithTimeout(ctx, time.Minute) // TODO:[KR] make this timeout configurable ?
		defer cancel()
		err = d.cacheClient.Modify(rctx, d.Config().Name, &cache.Opts{
			Store: store,
		}, nil, []*cache.Update{cUpd})
		if err != nil {
			log.Errorf("datastore %s failed to send modify request to cache: %v", d.config.Name, err)
		}
	}
}

type SdcpbUpdateDedup struct {
	lookup map[string]*sdcpb.Update
}

func NewSdcpbUpdateDedup() *SdcpbUpdateDedup {
	return &SdcpbUpdateDedup{
		lookup: map[string]*sdcpb.Update{},
	}
}

func (s *SdcpbUpdateDedup) AddUpdates(upds []*sdcpb.Update) {
	for _, upd := range upds {
		s.AddUpdate(upd)
	}
}

func (s *SdcpbUpdateDedup) AddUpdate(upd *sdcpb.Update) {
	path := upd.Path.String()
	if _, exists := s.lookup[path]; exists {
		return
	}
	s.lookup[path] = upd
}

func (s *SdcpbUpdateDedup) Updates() []*sdcpb.Update {
	result := make([]*sdcpb.Update, 0, len(s.lookup))

	for _, v := range s.lookup {
		result = append(result, v)
	}
	return result
}

// helper for GetSchema
func (d *Datastore) getSchema(ctx context.Context, p *sdcpb.Path) (*sdcpb.GetSchemaResponse, error) {
	return d.getValidationClient().GetSchema(ctx, p)
}

func (d *Datastore) validatePath(ctx context.Context, p *sdcpb.Path) error {
	_, err := d.getSchema(ctx, p)
	return err
}

func (d *Datastore) toPath(ctx context.Context, p []string) (*sdcpb.Path, error) {
	path, err := d.getValidationClient().ToPath(ctx, p)
	if err != nil {
		return nil, err
	}
	return path, nil
}

// getValidationClient will create a ValidationClient instance if not already existing
// save it as part of the datastore and return a valid *clients.ValidationClient
func (d *Datastore) getValidationClient() clients.ValidationClient {
	// if not initialized, init it, cache it
	if d._validationClientBound == nil {
		d._validationClientBound = clients.NewValidationClient(d.Name(), d.cacheClient, d.Schema().GetSchema(), d.schemaClient)
	}
	// return the bound validation client
	return d._validationClientBound
}

func (d *Datastore) WatchDeviations(req *sdcpb.WatchDeviationRequest, stream sdcpb.DataServer_WatchDeviationsServer) error {
	d.m.Lock()
	defer d.m.Unlock()

	ctx := stream.Context()
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "missing peer info")
	}
	pName := p.Addr.String()

	if oStream, ok := d.deviationClients[pName]; ok {
		_ = oStream // TODO:
	}

	d.deviationClients[pName] = stream
	return nil
}

func (d *Datastore) StopDeviationsWatch(peer string) {
	d.m.Lock()
	defer d.m.Unlock()
	delete(d.deviationClients, peer)
}

func (d *Datastore) DeviationMgr(ctx context.Context) {
	log.Infof("%s: starting deviationMgr...", d.Name())
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO: send deviation START
			d.m.RLock()
			// copy deviation streams
			dm := make(map[string]sdcpb.DataServer_WatchDeviationsServer)
			for n, devStream := range d.deviationClients {
				dm[n] = devStream
			}
			d.m.RUnlock()
			d.runDeviationUpdate(ctx, dm)
		}
	}
}

func (d *Datastore) runDeviationUpdate(ctx context.Context, dm map[string]sdcpb.DataServer_WatchDeviationsServer) {

	sep := "/"

	// send deviation START
	for _, dc := range dm {
		err := dc.Send(&sdcpb.WatchDeviationResponse{
			Name:  d.Name(),
			Event: sdcpb.DeviationEvent_START,
		})
		if err != nil {
			log.Errorf("%s: failed to send deviation start: %v", d.Name(), err)
			continue
		}
	}
	// collect intent deviations and store them for clearing
	newDeviations := make(map[string][]*sdcpb.WatchDeviationResponse)

	configPaths := map[string]struct{}{}

	// go through config and calculate deviations
	for upd := range d.cacheClient.ReadCh(ctx, d.Name(), &cache.Opts{Store: cachepb.Store_CONFIG}, [][]string{nil}, 0) {
		// save the updates path as an already checked path
		configPaths[strings.Join(upd.GetPath(), sep)] = struct{}{}

		v, err := upd.Value()
		if err != nil {
			log.Errorf("%s: failed to convert value: %v", d.Name(), err)
			continue
		}

		intentsUpdates := d.cacheClient.Read(ctx, d.Name(), &cache.Opts{
			Store:         cachepb.Store_INTENDED,
			Owner:         "",
			Priority:      0,
			PriorityCount: 0,
		}, [][]string{upd.GetPath()}, 0)
		if len(intentsUpdates) == 0 {
			log.Debugf("%s: has unhandled config %v: %v", d.Name(), upd.GetPath(), v)
			// TODO: generate an unhandled config deviation
			sp, err := d.toPath(ctx, upd.GetPath())
			if err != nil {
				log.Errorf("%s: failed to convert cached path to xpath: %v", d.Name(), err)
			}

			rsp := &sdcpb.WatchDeviationResponse{
				Name:         d.Name(),
				Intent:       upd.Owner(),
				Event:        sdcpb.DeviationEvent_UPDATE,
				Reason:       sdcpb.DeviationReason_UNHANDLED,
				Path:         sp,
				CurrentValue: v,
			}
			for _, dc := range dm {
				err = dc.Send(rsp)
				if err != nil {
					log.Errorf("%s: failed to send deviation: %v", d.Name(), err)
					continue
				}
			}
			continue
		}
		// NOT_APPLIED or OVERRULED deviation
		// sort intent updates by priority/TS
		sort.Slice(intentsUpdates, func(i, j int) bool {
			if intentsUpdates[i].Priority() == intentsUpdates[j].Priority() {
				return intentsUpdates[i].TS() < intentsUpdates[j].TS()
			}
			return intentsUpdates[i].Priority() < intentsUpdates[j].Priority()
		})
		// first intent
		// // compare values with config
		fiv, err := intentsUpdates[0].Value()
		if err != nil {
			log.Errorf("%s: failed to convert intent value: %v", d.Name(), err)
			continue
		}
		sp, err := d.toPath(ctx, intentsUpdates[0].GetPath())
		if err != nil {
			log.Errorf("%s: failed to convert path %v: %v", d.Name(), intentsUpdates[0].GetPath(), err)
			continue
		}
		scRsp, err := d.getSchema(ctx, sp)
		if err != nil {
			log.Errorf("%s: failed to get path schema: %v ", d.Name(), err)
			continue
		}
		nfiv, err := utils.TypedValueToYANGType(fiv, scRsp.GetSchema())
		if err != nil {
			log.Errorf("%s: failed to convert value to its YANG type: %v ", d.Name(), err)
			continue
		}
		if !utils.EqualTypedValues(nfiv, v) {
			log.Debugf("%s: intent %s has a NOT_APPLIED deviation: configured: %v -> expected %v",
				d.Name(), intentsUpdates[0].Owner(), v, nfiv)
			rsp := &sdcpb.WatchDeviationResponse{
				Name:          d.Name(),
				Intent:        intentsUpdates[0].Owner(),
				Event:         sdcpb.DeviationEvent_UPDATE,
				Reason:        sdcpb.DeviationReason_NOT_APPLIED,
				Path:          sp,
				ExpectedValue: nfiv,
				CurrentValue:  v,
			}
			for _, dc := range dm {
				err = dc.Send(rsp)
				if err != nil {
					log.Errorf("%s: failed to send deviation: %v", d.Name(), err)
					continue
				}
			}
			xp := utils.ToXPath(sp, false)
			if _, ok := newDeviations[xp]; !ok {
				newDeviations[xp] = make([]*sdcpb.WatchDeviationResponse, 0, 1)
			}
			newDeviations[xp] = append(newDeviations[xp], rsp)
		}
		// remaining intents
		for _, intUpd := range intentsUpdates[1:] {
			iv, err := intUpd.Value()
			if err != nil {
				log.Errorf("%s: failed to convert intent value: %v", d.Name(), err)
				continue
			}
			sp, err := d.toPath(ctx, intUpd.GetPath())
			if err != nil {
				log.Errorf("%s: failed to convert path %v: %v", d.Name(), intUpd.GetPath(), err)
				continue
			}
			scRsp, err := d.getSchema(ctx, sp)
			if err != nil {
				log.Errorf("%s: failed to get path schema: %v ", d.Name(), err)
				continue
			}
			niv, err := utils.TypedValueToYANGType(iv, scRsp.GetSchema())
			if err != nil {
				log.Errorf("%s: failed to convert value to its YANG type: %v ", d.Name(), err)
				continue
			}
			if !utils.EqualTypedValues(nfiv, niv) {
				log.Debugf("%s: intent %s has an OVERRULED deviation: ruling intent has: %v -> overruled intent has: %v",
					d.Name(), intUpd.Owner(), nfiv, niv)
				// TODO: generate an OVERRULED deviation

				rsp := &sdcpb.WatchDeviationResponse{
					Name:          d.Name(),
					Intent:        intUpd.Owner(),
					Event:         sdcpb.DeviationEvent_UPDATE,
					Reason:        sdcpb.DeviationReason_OVERRULED,
					Path:          sp,
					ExpectedValue: iv,
					CurrentValue:  fiv,
				}
				for _, dc := range dm {
					err = dc.Send(rsp)
					if err != nil {
						log.Errorf("%s: failed to send deviation: %v", d.Name(), err)
						continue
					}
				}
				xp := utils.ToXPath(sp, false)
				if _, ok := newDeviations[xp]; !ok {
					newDeviations[xp] = make([]*sdcpb.WatchDeviationResponse, 0, 1)
				}
				newDeviations[xp] = append(newDeviations[xp], rsp)
			}
		}
	}

	intendedUpdates, err := d.readStoreKeysMeta(ctx, cachepb.Store_INTENDED)
	if err != nil {
		log.Error(err)
		return
	}

	for _, upds := range intendedUpdates {
		for _, upd := range upds {
			path := strings.Join(upd.GetPath(), sep)
			if _, exists := configPaths[path]; !exists {

				// iv, err := upd.Value()
				// if err != nil {
				// 	log.Errorf("%s: failed to convert intent value: %v", d.Name(), err)
				// 	continue
				// }

				path, err := d.toPath(ctx, upd.GetPath())
				if err != nil {
					log.Error(err)
					continue
				}
				// scRsp, err := d.getSchema(ctx, path)
				// if err != nil {
				// 	log.Errorf("%s: failed to get path schema: %v ", d.Name(), err)
				// 	continue
				// }
				// niv, err := d.typedValueToYANGType(iv, scRsp.GetSchema())
				// if err != nil {
				// 	log.Errorf("%s: failed to convert value to its YANG type: %v ", d.Name(), err)
				// 	continue
				// }

				rsp := &sdcpb.WatchDeviationResponse{
					Name:          d.Name(),
					Intent:        upd.Owner(),
					Event:         sdcpb.DeviationEvent_UPDATE,
					Reason:        sdcpb.DeviationReason_NOT_APPLIED,
					Path:          path,
					ExpectedValue: nil, // TODO this need to be fixed
					CurrentValue:  nil,
				}
				for _, dc := range dm {
					err = dc.Send(rsp)
					if err != nil {
						log.Errorf("%s: failed to send deviation: %v", d.Name(), err)
						continue
					}
				}
			}
		}
	}

	// send deviation event END
	for _, dc := range dm {
		err := dc.Send(&sdcpb.WatchDeviationResponse{
			Name:  d.Name(),
			Event: sdcpb.DeviationEvent_END,
		})
		if err != nil {
			log.Errorf("%s: failed to send deviation end: %v", d.Name(), err)
			continue
		}
	}
	d.md.Lock()
	d.currentIntentsDeviations = newDeviations
	d.md.Unlock()
}

// DatastoreRollbackAdapter implements the types.RollbackInterface and encapsulates the Datastore.
type DatastoreRollbackAdapter struct {
	d *Datastore
}

// NewDatastoreRollbackAdapter constructor for the DatastoreRollbackAdapter.
func NewDatastoreRollbackAdapter(d *Datastore) *DatastoreRollbackAdapter {
	return &DatastoreRollbackAdapter{
		d: d,
	}
}

// TransactionRollback is adapted to the datastore.lowlevelTransactionSet() function
func (dra *DatastoreRollbackAdapter) TransactionRollback(ctx context.Context, transaction *types.Transaction, dryRun bool) (*sdcpb.TransactionSetResponse, error) {
	return dra.d.lowlevelTransactionSet(ctx, transaction, dryRun)
}

// Assure the types.RollbackInterface is implemented by the DatastoreRollbackAdapter
var _ types.RollbackInterface = &DatastoreRollbackAdapter{}
