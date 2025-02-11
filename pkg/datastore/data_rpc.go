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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sdcio/cache/proto/cachepb"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/tree"
	"github.com/sdcio/data-server/pkg/utils"
)

const (
	// to be used for candidates created without an owner
	DefaultOwner = "__sdcio"
)

func (d *Datastore) Get(ctx context.Context, req *sdcpb.GetDataRequest, nCh chan *sdcpb.GetDataResponse) error {
	defer close(nCh)
	switch req.GetDatastore().GetType() {
	case sdcpb.Type_MAIN:
	case sdcpb.Type_CANDIDATE:
	case sdcpb.Type_INTENDED:
		if req.GetDataType() == sdcpb.DataType_STATE {
			return status.Error(codes.InvalidArgument, "cannot query STATE data from INTENDED store")
		}
	}

	switch req.GetEncoding() {
	case sdcpb.Encoding_STRING:
	case sdcpb.Encoding_JSON:
	case sdcpb.Encoding_JSON_IETF:
	case sdcpb.Encoding_PROTO:
	default:
		return fmt.Errorf("unknown encoding: %v", req.GetEncoding())
	}

	var err error
	// validate that path(s) exist in the schema
	for _, p := range req.GetPath() {
		err = d.validatePath(ctx, p)
		if err != nil {
			return err
		}
	}

	// build target cache name
	name := req.GetName()
	if req.GetDatastore().GetName() != "" {
		name = fmt.Sprintf("%s/%s", req.GetName(), req.GetDatastore().GetName())
	}

	// convert sdcpb paths to a string list
	paths := make([][]string, 0, len(req.GetPath()))
	for _, p := range req.GetPath() {
		paths = append(paths, utils.ToStrings(p, false, false))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch req.GetEncoding() {
	case sdcpb.Encoding_STRING:
		err = d.handleGetDataUpdatesSTRING(ctx, name, req, paths, nCh)
	case sdcpb.Encoding_JSON:
		err = d.handleGetDataUpdatesJSON(ctx, name, req, paths, nCh, false)
	case sdcpb.Encoding_JSON_IETF:
		err = d.handleGetDataUpdatesJSON(ctx, name, req, paths, nCh, true)
	case sdcpb.Encoding_PROTO:
		err = d.handleGetDataUpdatesPROTO(ctx, name, req, paths, nCh)
	}
	if err != nil {
		return err
	}
	return nil
}

func (d *Datastore) handleGetDataUpdatesSTRING(ctx context.Context, name string, req *sdcpb.GetDataRequest, paths [][]string, out chan *sdcpb.GetDataResponse) error {
NEXT_STORE:
	for _, store := range getStores(req) {
		in := d.cacheClient.ReadCh(ctx, name, &cache.Opts{
			Store:    store,
			Owner:    req.GetDatastore().GetOwner(),
			Priority: req.GetDatastore().GetPriority(),
		}, paths, 0)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case upd, ok := <-in:
				//log.Debugf("ds=%s read path=%v from store=%v: %v", name, paths, store, upd)
				if !ok {
					continue NEXT_STORE
				}
				if len(upd.GetPath()) == 0 {
					continue
				}
				scp, err := d.toPath(ctx, upd.GetPath())
				if err != nil {
					return err
				}
				switch len(scp.GetElem()) {
				case 0:
					continue
				case 1:
					if scp.GetElem()[0].GetName() == "" {
						continue
					}
				}
				tv, err := upd.Value()
				if err != nil {
					return err
				}
				notification := &sdcpb.Notification{
					Timestamp: time.Now().UnixNano(),
					Update: []*sdcpb.Update{{
						Path:  scp,
						Value: tv,
					}},
				}
				rsp := &sdcpb.GetDataResponse{
					Notification: []*sdcpb.Notification{notification},
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- rsp:
				}
			}
		}
	}
	return nil
}

func (d *Datastore) handleGetDataUpdatesJSON(ctx context.Context, name string, req *sdcpb.GetDataRequest, paths [][]string, out chan *sdcpb.GetDataResponse, ietf bool) error {
	now := time.Now().UnixNano()

	treeSCC := tree.NewTreeSchemaCacheClient(d.Name(), d.cacheClient, d.getValidationClient())
	tc := tree.NewTreeContext(treeSCC, "")
	root, err := tree.NewTreeRoot(ctx, tc)
	if err != nil {
		return err
	}

	flagsExisting := tree.NewUpdateInsertFlags()

	for _, store := range getStores(req) {
		in := d.cacheClient.ReadCh(ctx, name, &cache.Opts{
			Store:    store,
			Owner:    req.GetDatastore().GetOwner(),
			Priority: req.GetDatastore().GetPriority(),
		}, paths, 0)
	OUTER:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case upd, ok := <-in:
				if !ok {
					break OUTER
				}

				if len(upd.GetPath()) == 0 {
					continue
				}

				scp, err := d.toPath(ctx, upd.GetPath())
				if err != nil {
					return err
				}
				switch len(scp.GetElem()) {
				case 0:
					continue
				case 1:
					if scp.GetElem()[0].GetName() == "" {
						continue
					}
				}
				root.AddCacheUpdateRecursive(ctx, upd, flagsExisting)
			}
		}
	}

	root.FinishInsertionPhase(ctx)

	var j any
	// marshal map into JSON bytes
	if ietf {
		j, err = root.ToJsonIETF(false)
		if err != nil {
			return err
		}
	} else {
		j, err = root.ToJson(false)
		if err != nil {
			return err
		}
	}
	b, err := json.Marshal(j)
	if err != nil {
		err = fmt.Errorf("failed json builder indent : %v", err)
		log.Error(err)
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- &sdcpb.GetDataResponse{
		Notification: []*sdcpb.Notification{
			{
				Timestamp: now,
				Update: []*sdcpb.Update{{
					Value: &sdcpb.TypedValue{Value: &sdcpb.TypedValue_JsonVal{JsonVal: b}},
				}},
			},
		},
	}:
	}
	return nil
}

func (d *Datastore) handleGetDataUpdatesPROTO(ctx context.Context, name string, req *sdcpb.GetDataRequest, paths [][]string, out chan *sdcpb.GetDataResponse) error {
	converter := utils.NewConverter(d.getValidationClient())
NEXT_STORE:
	for _, store := range getStores(req) {
		in := d.cacheClient.ReadCh(ctx, name, &cache.Opts{
			Store:    store,
			Owner:    req.GetDatastore().GetOwner(),
			Priority: req.GetDatastore().GetPriority(),
		}, paths, 0)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case upd, ok := <-in:
				//log.Debugf("ds=%s read path=%v from store=%v: %v", name, paths, store, upd)
				if !ok {
					continue NEXT_STORE
				}

				if len(upd.GetPath()) == 0 {
					continue
				}
				scp, err := d.toPath(ctx, upd.GetPath())
				if err != nil {
					return err
				}
				switch len(scp.GetElem()) {
				case 0:
					continue
				case 1:
					if scp.GetElem()[0].GetName() == "" {
						continue
					}
				}
				tv, err := upd.Value()
				if err != nil {
					return err
				}
				ctv, err := converter.ConvertTypedValueToProto(ctx, scp, tv)
				if err != nil {
					return err
				}
				notification := &sdcpb.Notification{
					Timestamp: time.Now().UnixNano(),
					Update: []*sdcpb.Update{{
						Path:  scp,
						Value: ctv,
					}},
				}
				rsp := &sdcpb.GetDataResponse{
					Notification: []*sdcpb.Notification{notification},
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- rsp:
				}
			}
		}
	}
	return nil
}

func (d *Datastore) Subscribe(req *sdcpb.SubscribeRequest, stream sdcpb.DataServer_SubscribeServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	var err error
	for _, subsc := range req.GetSubscription() {
		err := d.doSubscribeOnce(ctx, subsc, stream)
		if err != nil {
			return err
		}
	}
	err = stream.Send(&sdcpb.SubscribeResponse{
		Response: &sdcpb.SubscribeResponse_SyncResponse{
			SyncResponse: true,
		},
	})
	if err != nil {
		return err
	}
	// start periodic gets, TODO: optimize using cache RPC
	wg := new(sync.WaitGroup)
	wg.Add(len(req.GetSubscription()))
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})
	for _, subsc := range req.GetSubscription() {
		go func(subsc *sdcpb.Subscription) {
			ticker := time.NewTicker(time.Duration(subsc.GetSampleInterval()))
			defer ticker.Stop()
			defer wg.Done()
			for {
				select {
				case <-doneCh:
					return
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case <-ticker.C:
					err := d.doSubscribeOnce(ctx, subsc, stream)
					if err != nil {
						errCh <- err
						close(doneCh)
						return
					}
				}
			}
		}(subsc)
	}
	wg.Wait()
	return nil
}

func (d *Datastore) validateUpdate(ctx context.Context, upd *sdcpb.Update) error {
	// 1.validate the path i.e check that the path exists
	// 2.validate that the value is compliant with the schema

	// 1. validate the path
	rsp, err := d.getSchema(ctx, upd.GetPath())
	if err != nil {
		return err
	}
	// 2. convert value to its YANG type
	upd.Value, err = utils.ConvertTypedValueToYANGType(rsp.GetSchema(), upd.GetValue())
	if err != nil {
		return err
	}
	// 2. validate value
	val, err := utils.GetSchemaValue(upd.GetValue())
	if err != nil {
		return err
	}
	switch obj := rsp.GetSchema().Schema.(type) {
	case *sdcpb.SchemaElem_Container:
		if !pathIsKeyAsLeaf(upd.GetPath()) && !obj.Container.IsPresence {
			return fmt.Errorf("cannot set value on container %q object", obj.Container.Name)
		}
		// TODO: validate key as leaf
	case *sdcpb.SchemaElem_Field:
		if obj.Field.IsState {
			return fmt.Errorf("cannot set state field: %v", obj.Field.Name)
		}
		err = validateFieldValue(obj.Field, val)
		if err != nil {
			return err
		}
	case *sdcpb.SchemaElem_Leaflist:
		err = validateLeafListValue(obj.Leaflist, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateFieldValue(f *sdcpb.LeafSchema, v any) error {
	return validateLeafTypeValue(f.GetType(), v)
}

func validateLeafTypeValue(lt *sdcpb.SchemaLeafType, v any) error {
	switch lt.GetType() {
	case "string":
		// TODO: validate length and range
		return nil
	case "int8":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseInt(v, 10, 8)
			if err != nil {
				return err
			}
		case int64:
			if v > math.MaxInt8 || v < math.MinInt8 {
				return fmt.Errorf("value %v out of bound for type %s", v, lt.GetType())
			}
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "int16":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseInt(v, 10, 16)
			if err != nil {
				return err
			}
		case int64:
			if v > math.MaxInt16 || v < math.MinInt16 {
				return fmt.Errorf("value %v out of bound for type %s", v, lt.GetType())
			}
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "int32":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return err
			}
		case int64:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return fmt.Errorf("value %v out of bound for type %s", v, lt.GetType())
			}
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "int64":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return err
			}
		case int64:
			// No need to do anything, same type
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "uint8":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseUint(v, 10, 8)
			if err != nil {
				return err
			}
		case uint64:
			if v > math.MaxUint8 {
				return fmt.Errorf("value %v out of bound for type %s", v, lt.GetType())
			}
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "uint16":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseUint(v, 10, 16)
			if err != nil {
				return err
			}
		case uint64:
			if v > math.MaxUint16 {
				return fmt.Errorf("value %v out of bound for type %s", v, lt.GetType())
			}
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "uint32":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseUint(v, 10, 32)
			if err != nil {
				return err
			}
		case uint64:
			if v > math.MaxUint32 {
				return fmt.Errorf("value %v out of bound for type %s", v, lt.GetType())
			}
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "uint64":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return err
			}
		case uint64:
			// No need to do anything, same type
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "boolean":
		switch v := v.(type) {
		case string:
			_, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("value %v must be a boolean: %v", v, err)
			}
		case bool:
			return nil
		default:
			return fmt.Errorf("unexpected casted type %T in %v", v, lt.GetType())
		}
		return nil
	case "enumeration":
		valid := false
		for _, vv := range lt.EnumNames {
			if fmt.Sprintf("%s", v) == vv {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("value %q does not match enum type %q, must be one of [%s]", v, lt.TypeName, strings.Join(lt.EnumNames, ", "))
		}
		return nil
	case "union":
		valid := false
		for _, ut := range lt.GetUnionTypes() {
			err := validateLeafTypeValue(ut, v)
			if err == nil {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("value %v does not match union type %v", v, lt.TypeName)
		}
		return nil
	case "identityref":
		valid := false
		identities := make([]string, 0, len(lt.IdentityPrefixesMap))
		for vv := range lt.IdentityPrefixesMap {
			identities = append(identities, vv)
			if fmt.Sprintf("%s", v) == vv {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("value %q does not match identityRef type %q, must be one of [%s]", v, lt.TypeName, strings.Join(identities, ", "))
		}
		return nil
	case "decimal64":
		switch v := v.(type) {
		case float64: // if it's a float64 then it's a valid decimal64
		case string:
			if c := strings.Count(v, "."); c == 0 || c > 1 {
				return fmt.Errorf("value %q is not a valid Decimal64", v)
			}
		case sdcpb.Decimal64, *sdcpb.Decimal64:
			// No need to do anything, same type
		default:
			return fmt.Errorf("unexpected type for a Decimal64 value %q: %T", v, v)
		}
		return nil
	case "leafref":
		// TODO: does this need extra validation?
		return nil
	case "empty":
		switch v.(type) {
		case *emptypb.Empty:
			return nil
		}
		return fmt.Errorf("value %v is not an empty JSON object '{}' so does not match empty type", v)
	default:
		return fmt.Errorf("unhandled type %v for value %q", lt.GetType(), v)
	}
}

func validateLeafListValue(ll *sdcpb.LeafListSchema, v any) error {
	switch vTyped := v.(type) {
	case *sdcpb.ScalarArray:
		for _, elem := range vTyped.Element {
			val, err := utils.GetSchemaValue(elem)
			if err != nil {
				return err
			}
			err = validateLeafTypeValue(ll.GetType(), val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *Datastore) doSubscribeOnce(ctx context.Context, subscription *sdcpb.Subscription, stream sdcpb.DataServer_SubscribeServer) error {
	paths := make([][]string, 0, len(subscription.GetPath()))
	for _, path := range subscription.GetPath() {
		paths = append(paths, utils.ToStrings(path, false, false))
	}

	for _, store := range getStores(subscription) {
		for upd := range d.cacheClient.ReadCh(ctx, d.config.Name, &cache.Opts{
			Store: store,
		}, paths, 0) {
			log.Debugf("ds=%s read path=%v from store=%v: %v", d.config.Name, paths, store, upd)
			rsp, err := d.subscribeResponseFromCacheUpdate(ctx, upd)
			if err != nil {
				return err
			}
			log.Debugf("ds=%s sending subscribe response: %v", d.config.Name, rsp)
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				err = stream.Send(rsp)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getStores(req proto.Message) []cachepb.Store {
	var dt sdcpb.DataType
	var candName string
	switch req := req.(type) {
	case *sdcpb.GetDataRequest:
		if req.GetDatastore().GetType() == sdcpb.Type_INTENDED {
			return []cachepb.Store{cachepb.Store_INTENDED}
		}
		dt = req.GetDataType()
		candName = req.GetDatastore().GetName()
	case *sdcpb.Subscription:
		dt = req.GetDataType()
	}

	var stores []cachepb.Store
	switch dt {
	case sdcpb.DataType_ALL:
		stores = []cachepb.Store{cachepb.Store_CONFIG}
		if candName == "" {
			stores = append(stores, cachepb.Store_STATE)
		}
	case sdcpb.DataType_CONFIG:
		stores = []cachepb.Store{cachepb.Store_CONFIG}
	case sdcpb.DataType_STATE:
		if candName == "" {
			stores = []cachepb.Store{cachepb.Store_STATE}
		}
	}
	return stores
}

func (d *Datastore) subscribeResponseFromCacheUpdate(ctx context.Context, upd *cache.Update) (*sdcpb.SubscribeResponse, error) {
	scp, err := d.toPath(ctx, upd.GetPath())
	if err != nil {
		return nil, err
	}
	tv, err := upd.Value()
	if err != nil {
		return nil, err
	}
	notification := &sdcpb.Notification{
		Timestamp: time.Now().UnixNano(),
		Update: []*sdcpb.Update{{
			Path:  scp,
			Value: tv,
		}},
	}
	return &sdcpb.SubscribeResponse{
		Response: &sdcpb.SubscribeResponse_Update{
			Update: notification,
		},
	}, nil
}

// func (d *Datastore) setCandidate(ctx context.Context, req *sdcpb.SetDataRequest, expandDeletes bool) (*sdcpb.SetDataResponse, error) {
// 	ok, err := d.cacheClient.HasCandidate(ctx, req.GetName(), req.GetDatastore().GetName())
// 	if err != nil {
// 		return nil, err
// 	}
// 	if !ok {
// 		return nil, status.Errorf(codes.InvalidArgument, "unknown candidate %s", req.GetDatastore().GetName())
// 	}

// 	replaces := make([]*sdcpb.Update, 0, len(req.GetReplace()))
// 	updates := make([]*sdcpb.Update, 0, len(req.GetUpdate()))

// 	converter := utils.NewConverter(d.getValidationClient())

// 	// expand json/json_ietf values
// 	for _, upd := range req.GetReplace() {
// 		rs, err := converter.ExpandUpdate(ctx, upd, false)
// 		if err != nil {
// 			return nil, status.Errorf(codes.InvalidArgument, "failed expand replace: %v", err)
// 		}
// 		replaces = append(replaces, rs...)
// 	}

// 	for _, upd := range req.GetUpdate() {
// 		rs, err := converter.ExpandUpdate(ctx, upd, false)
// 		if err != nil {
// 			return nil, status.Errorf(codes.InvalidArgument, "failed expand update: %v", err)
// 		}
// 		updates = append(updates, rs...)
// 	}

// 	// debugging
// 	if log.GetLevel() >= log.DebugLevel {
// 		for _, upd := range replaces {
// 			log.Debugf("expanded replace:\n%s", prototext.Format(upd))
// 		}
// 		for _, upd := range updates {
// 			log.Debugf("expanded update:\n%s", prototext.Format(upd))
// 		}
// 	}

// 	// validate individual deletes
// 	dels := make([]*sdcpb.Path, 0, len(req.GetDelete()))
// 	for _, del := range req.GetDelete() {
// 		if expandDeletes {
// 			rsp, err := d.schemaClient.ExpandPath(ctx, &sdcpb.ExpandPathRequest{
// 				Path:     del,
// 				Schema:   d.Schema().GetSchema(),
// 				DataType: sdcpb.DataType_CONFIG,
// 			})
// 			if err != nil {
// 				return nil, status.Errorf(codes.InvalidArgument, "delete path: %q validation failed: %v", del, err)
// 			}
// 			dels = append(dels, rsp.GetPath()...)
// 			continue
// 		}
// 		err = d.validatePath(ctx, del)
// 		if err != nil {
// 			return nil, err
// 		}
// 		dels = append(dels, del)
// 	}

// 	for _, upd := range replaces {
// 		err = d.validateUpdate(ctx, upd)
// 		if err != nil {
// 			log.Debugf("replace %v validation failed: %v", upd, err)
// 			return nil, status.Errorf(codes.InvalidArgument, "replace: validation failed: %v", err)
// 		}
// 	}

// 	for _, upd := range updates {
// 		err = d.validateUpdate(ctx, upd)
// 		if err != nil {
// 			log.Debugf("update %v validation failed: %v", upd, err)
// 			return nil, status.Errorf(codes.InvalidArgument, "update: validation failed: %v", err)
// 		}
// 	}

// 	// insert/delete
// 	// the order of operations is delete, replace, update
// 	rsp := &sdcpb.SetDataResponse{
// 		Response: make([]*sdcpb.UpdateResult, 0,
// 			len(dels)+len(replaces)+len(updates)),
// 	}

// 	name := fmt.Sprintf("%s/%s", req.GetName(), req.GetDatastore().GetName())
// 	cdels := make([][]string, 0, len(dels))
// 	upds := make([]*cache.Update, 0, len(replaces)+len(updates))
// 	// deletes start
// 	for _, del := range dels {
// 		cdels = append(cdels, utils.ToStrings(del, false, false))
// 	}
// 	for _, changes := range [][]*sdcpb.Update{replaces, updates} {
// 		for _, upd := range changes {
// 			cUpd, err := d.cacheClient.NewUpdate(upd)
// 			if err != nil {
// 				return nil, err
// 			}
// 			upds = append(upds, cUpd)
// 		}
// 	}
// 	err = d.cacheClient.Modify(ctx, name, &cache.Opts{
// 		Store: cachepb.Store_CONFIG,
// 	}, cdels, upds)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// deletes start
// 	for _, del := range req.GetDelete() {
// 		rsp.Response = append(rsp.Response, &sdcpb.UpdateResult{
// 			Path: del,
// 			Op:   sdcpb.UpdateResult_DELETE,
// 		})
// 	}
// 	// deletes end
// 	// replaces start
// 	for _, rep := range req.GetReplace() {
// 		rsp.Response = append(rsp.Response, &sdcpb.UpdateResult{
// 			Path: rep.GetPath(),
// 			Op:   sdcpb.UpdateResult_REPLACE,
// 		})
// 	}
// 	// replaces end
// 	// updates start
// 	for _, upd := range req.GetUpdate() {
// 		rsp.Response = append(rsp.Response, &sdcpb.UpdateResult{
// 			Path: upd.GetPath(),
// 			Op:   sdcpb.UpdateResult_UPDATE,
// 		})
// 	}
// 	// updates end
// 	return rsp, nil
// }
