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

package target

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/openconfig/gnmi/proto/gnmi"
	gapi "github.com/openconfig/gnmic/pkg/api"
	gtarget "github.com/openconfig/gnmic/pkg/target"
	"github.com/openconfig/gnmic/pkg/types"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/sdcio/data-server/pkg/config"
	"github.com/sdcio/data-server/pkg/utils"
)

const (
	syncRetryWaitTime = 10 * time.Second
)

type gnmiTarget struct {
	target    *gtarget.Target
	encodings map[gnmi.Encoding]struct{}
	cfg       *config.SBI
}

func newGNMITarget(ctx context.Context, name string, cfg *config.SBI, opts ...grpc.DialOption) (*gnmiTarget, error) {
	tc := &types.TargetConfig{
		Name:       name,
		Address:    fmt.Sprintf("%s:%d", cfg.Address, cfg.Port),
		Timeout:    10 * time.Second,
		RetryTimer: 2 * time.Second,
		BufferSize: 100,
	}
	if cfg.Credentials != nil {
		tc.Username = &cfg.Credentials.Username
		tc.Password = &cfg.Credentials.Password
	}
	if cfg.TLS != nil {
		tc.TLSCA = &cfg.TLS.CA
		tc.TLSCert = &cfg.TLS.Cert
		tc.TLSKey = &cfg.TLS.Key
		tc.SkipVerify = &cfg.TLS.SkipVerify
	} else {
		tc.Insecure = pointer.ToBool(true)
	}
	gt := &gnmiTarget{
		target:    gtarget.NewTarget(tc),
		encodings: make(map[gnmi.Encoding]struct{}),
		cfg:       cfg,
	}
	err := gt.target.CreateGNMIClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	// discover supported encodings
	capResp, err := gt.target.Capabilities(ctx)
	if err != nil {
		return nil, err
	}
	for _, enc := range capResp.GetSupportedEncodings() {
		gt.encodings[enc] = struct{}{}
	}

	if _, exists := gt.encodings[gnmi.Encoding(encoding(cfg.GnmiOptions.Encoding))]; !exists {
		return nil, fmt.Errorf("encoding %q not supported", cfg.GnmiOptions.Encoding)
	}

	return gt, nil
}

// sdcpbDataTypeToGNMIType helper to convert the sdcpb data type to the gnmi data type
func sdcpbDataTypeToGNMIType(x sdcpb.DataType) (gnmi.GetRequest_DataType, error) {
	switch x {
	case sdcpb.DataType_ALL:
		return gnmi.GetRequest_ALL, nil
	case sdcpb.DataType_CONFIG:
		return gnmi.GetRequest_CONFIG, nil
	case sdcpb.DataType_STATE:
		return gnmi.GetRequest_STATE, nil
	}
	return 9999, fmt.Errorf("unable to convert sdcpb DataType %s to gnmi DataType", x)
}

// sdcpbEncodingToGNMIENcoding helper to convert sdcpb encoding to gnmi encoding
func sdcpbEncodingToGNMIENcoding(x sdcpb.Encoding) (gnmi.Encoding, error) {
	switch x {
	case sdcpb.Encoding_JSON:
		return gnmi.Encoding_JSON, nil
	case sdcpb.Encoding_JSON_IETF:
		return gnmi.Encoding_JSON_IETF, nil
	case sdcpb.Encoding_PROTO:
		return gnmi.Encoding_PROTO, nil
	case sdcpb.Encoding_STRING:
		return gnmi.Encoding_ASCII, nil
	}
	return 9999, fmt.Errorf("unable to convert sdcpb encoding %s to gnmi encoding", x)
}

func (t *gnmiTarget) Get(ctx context.Context, req *sdcpb.GetDataRequest) (*sdcpb.GetDataResponse, error) {
	var err error
	gnmiReq := &gnmi.GetRequest{
		Path: make([]*gnmi.Path, 0, len(req.GetPath())),
	}
	for _, p := range req.GetPath() {
		gnmiReq.Path = append(gnmiReq.Path, utils.ToGNMIPath(p))
	}

	// convert sdcpb data type to gnmi data type
	gnmiReq.Type, err = sdcpbDataTypeToGNMIType(req.GetDataType())
	if err != nil {
		return nil, err
	}

	// convert sdcpb encoding to gnmi encoding
	gnmiReq.Encoding, err = sdcpbEncodingToGNMIENcoding(req.Encoding)
	if err != nil {
		return nil, err
	}
	// execute the gnmi get
	gnmiRsp, err := t.target.Get(ctx, gnmiReq)
	if err != nil {
		return nil, err
	}
	schemaRsp := &sdcpb.GetDataResponse{
		Notification: make([]*sdcpb.Notification, 0, len(gnmiRsp.GetNotification())),
	}
	for _, n := range gnmiRsp.GetNotification() {
		sn := utils.ToSchemaNotification(n)
		schemaRsp.Notification = append(schemaRsp.Notification, sn)
	}
	return schemaRsp, nil
}

func (t *gnmiTarget) Set(ctx context.Context, source TargetSource) (*sdcpb.SetDataResponse, error) {
	var upds []*sdcpb.Update
	var deletes []*sdcpb.Path
	var err error

	if t == nil {
		return nil, fmt.Errorf("%s", "not connected")
	}

	switch strings.ToLower(t.cfg.GnmiOptions.Encoding) {
	case "json":
		jsonData, err := source.ToJson(true)
		if err != nil {
			return nil, err
		}
		if jsonData != nil {
			jsonBytes, err := json.Marshal(jsonData)
			if err != nil {
				return nil, err
			}
			upds = []*sdcpb.Update{{Path: &sdcpb.Path{}, Value: &sdcpb.TypedValue{Value: &sdcpb.TypedValue_JsonVal{JsonVal: jsonBytes}}}}
		}
		// deletes from protos
		deletes, err = source.ToProtoDeletes(ctx)
		if err != nil {
			return nil, err
		}

	case "json_ietf":
		jsonData, err := source.ToJsonIETF(true)
		if err != nil {
			return nil, err
		}
		if jsonData != nil {
			jsonBytes, err := json.Marshal(jsonData)
			if err != nil {
				return nil, err
			}
			upds = []*sdcpb.Update{{Path: &sdcpb.Path{}, Value: &sdcpb.TypedValue{Value: &sdcpb.TypedValue_JsonIetfVal{JsonIetfVal: jsonBytes}}}}
		}
		// deletes from protos
		deletes, err = source.ToProtoDeletes(ctx)
		if err != nil {
			return nil, err
		}

	case "proto":
		upds, err = source.ToProtoUpdates(ctx, true)
		if err != nil {
			return nil, err
		}
		deletes, err = source.ToProtoDeletes(ctx)
		if err != nil {
			return nil, err
		}
	}

	setReq := &gnmi.SetRequest{
		Delete: make([]*gnmi.Path, 0, len(deletes)),
		Update: make([]*gnmi.Update, 0, len(upds)),
	}
	for _, del := range deletes {
		gdel := utils.ToGNMIPath(del)
		setReq.Delete = append(setReq.Delete, gdel)
	}
	for _, upd := range upds {
		gupd := t.convertKeyUpdates(upd)
		setReq.Update = append(setReq.Update, gupd)
	}

	log.Debugf("gnmi set request:\n%s", prototext.Format(setReq))

	rsp, err := t.target.Set(ctx, setReq)
	if err != nil {
		return nil, err
	}
	schemaSetRsp := &sdcpb.SetDataResponse{
		Response:  make([]*sdcpb.UpdateResult, 0, len(rsp.GetResponse())),
		Timestamp: rsp.GetTimestamp(),
	}
	for _, updr := range rsp.GetResponse() {
		schemaSetRsp.Response = append(schemaSetRsp.Response, &sdcpb.UpdateResult{
			Path: utils.FromGNMIPath(rsp.GetPrefix(), updr.GetPath()),
			Op:   sdcpb.UpdateResult_Operation(updr.GetOp()),
		})
	}
	return schemaSetRsp, nil
}

func (t *gnmiTarget) Status() *TargetStatus {
	result := NewTargetStatus(TargetStatusNotConnected)

	if t == nil || t.target == nil {
		result.Details = "connection not initialized"
		return result
	}
	switch t.target.ConnState() {
	case connectivity.Ready.String(), connectivity.Idle.String():
		result.Status = TargetStatusConnected
		result.Details = t.target.ConnState()
	case connectivity.Connecting.String(), connectivity.Shutdown.String(), connectivity.TransientFailure.String():
		result.Status = TargetStatusNotConnected
		result.Details = t.target.ConnState()
	}

	return result
}

func (t *gnmiTarget) Sync(octx context.Context, syncConfig *config.Sync, syncCh chan *SyncUpdate) {
	if t != nil && t.target != nil && t.target.Config != nil {
		log.Infof("starting target %s sync", t.target.Config.Name)
	}
	var cancel context.CancelFunc
	var ctx context.Context
	var err error
START:
	if cancel != nil {
		cancel()
	}
	ctx, cancel = context.WithCancel(octx)
	defer cancel()

	// todo: do not run read subscriptions for GET
	for _, gnmiSync := range syncConfig.Config {
		switch gnmiSync.Mode {
		case "once":
			err = t.periodicSync(ctx, gnmiSync)
		case "get":
			err = t.getSync(ctx, gnmiSync, syncCh)
		default:
			err = t.streamSync(ctx, gnmiSync)
		}
		if err != nil {
			log.Errorf("target=%s: failed to sync: %v", t.target.Config.Name, err)
			time.Sleep(syncRetryWaitTime)
			goto START
		}
	}

	defer t.target.StopSubscriptions()

	rspch, errCh := t.target.ReadSubscriptions()
	for {
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				log.Errorf("datastore %s sync stopped: %v", t.target.Config.Name, ctx.Err())
			}
			return
		case rsp := <-rspch:
			switch r := rsp.Response.Response.(type) {
			case *gnmi.SubscribeResponse_Update:
				syncCh <- &SyncUpdate{
					Store:  rsp.SubscriptionName,
					Update: utils.ToSchemaNotification(r.Update),
				}
			}
		case err := <-errCh:
			if err.Err != nil {
				t.target.StopSubscriptions()
				log.Errorf("%s: sync subscription failed: %v", t.target.Config.Name, err)
				time.Sleep(time.Second)
				goto START
			}
		}
	}
}

func (t *gnmiTarget) Close() error {
	if t == nil {
		return nil
	}
	if t.target == nil {
		return nil
	}
	return t.target.Close()
}

func sdcpbEncoding(e string) int {
	enc, ok := sdcpb.Encoding_value[strings.ToUpper(e)]
	if ok {
		return int(enc)
	}
	en, err := strconv.Atoi(e)
	if err != nil {
		return 0
	}
	return en
}

func encoding(e string) int {
	enc, ok := gnmi.Encoding_value[strings.ToUpper(e)]
	if ok {
		return int(enc)
	}
	en, err := strconv.Atoi(e)
	if err != nil {
		return 0
	}
	return en
}

func (t *gnmiTarget) getSync(ctx context.Context, gnmiSync *config.SyncProtocol, syncCh chan *SyncUpdate) error {
	// iterate syncConfig
	paths := make([]*sdcpb.Path, 0, len(gnmiSync.Paths))
	// iterate referenced paths
	for _, p := range gnmiSync.Paths {
		path, err := utils.ParsePath(p)
		if err != nil {
			return err
		}
		// add the parsed path
		paths = append(paths, path)
	}

	req := &sdcpb.GetDataRequest{
		Name:     gnmiSync.Name,
		Path:     paths,
		DataType: sdcpb.DataType_CONFIG,
		Datastore: &sdcpb.DataStore{
			Type: sdcpb.Type_MAIN,
		},
		Encoding: sdcpb.Encoding(sdcpbEncoding(gnmiSync.Encoding)),
	}

	go t.internalGetSync(ctx, req, syncCh)

	go func() {
		ticker := time.NewTicker(gnmiSync.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.internalGetSync(ctx, req, syncCh)
			}
		}
	}()

	return nil
}

func (t *gnmiTarget) internalGetSync(ctx context.Context, req *sdcpb.GetDataRequest, syncCh chan *SyncUpdate) {
	// execute gnmi get
	resp, err := t.Get(ctx, req)
	if err != nil {
		log.Errorf("sync error: %v", err)
		return
	}

	// push notifications into syncCh
	syncCh <- &SyncUpdate{
		Start: true,
	}
	notificationsCount := 0
	for _, n := range resp.GetNotification() {
		syncCh <- &SyncUpdate{
			Update: n,
		}
		notificationsCount++
	}
	log.Debugf("%s: synced %d notifications", t.target.Config.Name, notificationsCount)
	syncCh <- &SyncUpdate{
		End: true,
	}
}

func (t *gnmiTarget) periodicSync(ctx context.Context, gnmiSync *config.SyncProtocol) error {
	opts := make([]gapi.GNMIOption, 0)
	subscriptionOpts := make([]gapi.GNMIOption, 0)
	for _, p := range gnmiSync.Paths {
		subscriptionOpts = append(subscriptionOpts, gapi.Path(p))
	}
	opts = append(opts,
		gapi.EncodingCustom(encoding(gnmiSync.Encoding)),
		gapi.SubscriptionListModeONCE(),
		gapi.Subscription(subscriptionOpts...),
	)
	subReq, err := gapi.NewSubscribeRequest(opts...)
	if err != nil {
		return err
	}
	// initial subscribe ONCE
	go t.target.Subscribe(ctx, subReq, gnmiSync.Name)
	// periodic subscribe ONCE
	go func(gnmiSync *config.SyncProtocol) {
		ticker := time.NewTicker(gnmiSync.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				t.target.Subscribe(ctx, subReq, gnmiSync.Name)
			}
		}
	}(gnmiSync)
	return nil
}

func (t *gnmiTarget) streamSync(ctx context.Context, gnmiSync *config.SyncProtocol) error {
	opts := make([]gapi.GNMIOption, 0)
	subscriptionOpts := make([]gapi.GNMIOption, 0)
	for _, p := range gnmiSync.Paths {
		subscriptionOpts = append(subscriptionOpts, gapi.Path(p))
	}
	switch gnmiSync.Mode {
	case "sample":
		subscriptionOpts = append(subscriptionOpts, gapi.SubscriptionModeSAMPLE())
	case "on-change":
		subscriptionOpts = append(subscriptionOpts, gapi.SubscriptionModeON_CHANGE())
	}

	if gnmiSync.Interval > 0 {
		subscriptionOpts = append(subscriptionOpts, gapi.SampleInterval(gnmiSync.Interval))
	}
	opts = append(opts,
		gapi.EncodingCustom(encoding(gnmiSync.Encoding)),
		gapi.SubscriptionListModeSTREAM(),
		gapi.Subscription(subscriptionOpts...),
	)
	subReq, err := gapi.NewSubscribeRequest(opts...)
	if err != nil {
		return err

	}
	log.Infof("sync %q: subRequest: %v", gnmiSync.Name, subReq)
	go t.target.Subscribe(ctx, subReq, gnmiSync.Name)
	return nil
}

func (t *gnmiTarget) convertKeyUpdates(upd *sdcpb.Update) *gnmi.Update {
	if !pathIsKeyAsLeaf(upd.GetPath()) {
		return &gnmi.Update{
			Path: utils.ToGNMIPath(upd.GetPath()),
			Val:  utils.ToGNMITypedValue(upd.GetValue()),
		}
	}
	// convert key as leaf to jsonVal
	numPElem := len(upd.GetPath().GetElem())
	key := upd.GetPath().GetElem()[numPElem-1].GetName()
	valm := map[string]string{
		key: utils.TypedValueToString(upd.GetValue()),
	}
	b, _ := json.Marshal(valm)
	var val *sdcpb.TypedValue
	if _, ok := t.encodings[gnmi.Encoding_JSON_IETF]; ok {
		val = &sdcpb.TypedValue{Value: &sdcpb.TypedValue_JsonIetfVal{
			JsonIetfVal: b,
		}}
	} else if _, ok := t.encodings[gnmi.Encoding_JSON]; ok {
		val = &sdcpb.TypedValue{Value: &sdcpb.TypedValue_JsonVal{
			JsonVal: b,
		}}
	}

	// modify path
	p := proto.Clone(upd.GetPath()).(*sdcpb.Path)
	// p := upd.GetPath()
	p.Elem = p.GetElem()[:numPElem-1]
	// TODO: REVISIT: remove key from the last elem
	// delete(p.GetElem()[len(upd.GetPath().GetElem())-1].Key, key)
	return &gnmi.Update{
		Path: utils.ToGNMIPath(p),
		Val:  utils.ToGNMITypedValue(val),
	}
}

func pathIsKeyAsLeaf(p *sdcpb.Path) bool {
	numPElem := len(p.GetElem())
	if numPElem < 2 {
		return false
	}

	_, ok := p.GetElem()[numPElem-2].GetKey()[p.GetElem()[numPElem-1].GetName()]
	return ok
}
