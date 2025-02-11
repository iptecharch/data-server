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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/beevik/etree"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"

	"github.com/sdcio/data-server/pkg/config"
	schemaClient "github.com/sdcio/data-server/pkg/datastore/clients/schema"
	"github.com/sdcio/data-server/pkg/datastore/target/netconf"
	"github.com/sdcio/data-server/pkg/datastore/target/netconf/driver/scrapligo"
	"github.com/sdcio/data-server/pkg/utils"
)

type ncTarget struct {
	name   string
	driver netconf.Driver

	m *sync.Mutex

	schemaClient     schemaClient.SchemaClientBound
	sbiConfig        *config.SBI
	xml2sdcpbAdapter *netconf.XML2sdcpbConfigAdapter
}

func newNCTarget(_ context.Context, name string, cfg *config.SBI, schemaClient schemaClient.SchemaClientBound) (*ncTarget, error) {
	t := &ncTarget{
		name:             name,
		m:                new(sync.Mutex),
		schemaClient:     schemaClient,
		sbiConfig:        cfg,
		xml2sdcpbAdapter: netconf.NewXML2sdcpbConfigAdapter(schemaClient),
	}
	var err error
	// create a new NETCONF driver
	t.driver, err = scrapligo.NewScrapligoNetconfTarget(cfg)
	if err != nil {
		return t, err
	}
	return t, nil
}

func (t *ncTarget) Get(ctx context.Context, req *sdcpb.GetDataRequest) (*sdcpb.GetDataResponse, error) {
	if !t.Status().IsConnected() {
		return nil, fmt.Errorf("not connected")
	}
	var source string

	switch req.Datastore.Type {
	case sdcpb.Type_MAIN:
		source = "running"
	case sdcpb.Type_CANDIDATE:
		source = "candidate"
	}

	// init a new XMLConfigBuilder for the pathfilter
	pathfilterXmlBuilder := netconf.NewXMLConfigBuilder(t.schemaClient,
		&netconf.XMLConfigBuilderOpts{
			HonorNamespace:         t.sbiConfig.NetconfOptions.IncludeNS,
			OperationWithNamespace: t.sbiConfig.NetconfOptions.OperationWithNamespace,
			UseOperationRemove:     t.sbiConfig.NetconfOptions.UseOperationRemove,
		})

	// add all the requested paths to the document
	for _, p := range req.Path {
		err := pathfilterXmlBuilder.AddElements(ctx, p)
		if err != nil {
			return nil, err
		}
	}

	// retrieve the xml filter as string
	filterDoc, err := pathfilterXmlBuilder.GetDoc()
	if err != nil {
		return nil, err
	}
	log.Debugf("netconf filter:\n%s", filterDoc)

	// execute the GetConfig rpc
	ncResponse, err := t.driver.GetConfig(source, filterDoc)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			t.Close()
			go t.reconnect()
		}
		return nil, err
	}

	log.Debugf("netconf response:\n%s", ncResponse.DocAsString())

	// cmlImport := xml.NewXmlTreeImporter(ncResponse.Doc.Root())

	// treeCacheSchemaClient := tree.NewTreeSchemaCacheClient(t.name, nil, d.getValidationClient())
	// tc := tree.NewTreeContext(treeCacheSchemaClient, tree.RunningIntentName)

	// NewTreeRoot

	// start transformation, which yields the sdcpb_Notification
	noti, err := t.xml2sdcpbAdapter.Transform(ctx, ncResponse.Doc)
	if err != nil {
		return nil, err
	}

	// building the resulting sdcpb.GetDataResponse struct
	result := &sdcpb.GetDataResponse{
		Notification: noti,
	}
	return result, nil
}

func (t *ncTarget) Set(ctx context.Context, source TargetSource) (*sdcpb.SetDataResponse, error) {
	if t.Status().IsConnected() {
		return nil, fmt.Errorf("not connected")
	}

	switch t.sbiConfig.NetconfOptions.CommitDatastore {
	case "running":
		return t.setRunning(source)
	case "candidate":
		return t.setCandidate(source)
	}
	// should not get here if the config validation happened.
	return nil, fmt.Errorf("unknown commit-datastore: %s", t.sbiConfig.NetconfOptions.CommitDatastore)
}

func (t *ncTarget) Status() *TargetStatus {
	result := NewTargetStatus(TargetStatusNotConnected)
	if t == nil || t.driver == nil {
		result.Details = "connection not initialized"
		return result
	}
	if t.driver.IsAlive() {
		result.Status = TargetStatusConnected
	}
	return result
}

func (t *ncTarget) Sync(ctx context.Context, syncConfig *config.Sync, syncCh chan *SyncUpdate) {
	log.Infof("starting target %s [%s] sync", t.name, t.sbiConfig.Address)

	for _, ncc := range syncConfig.Config {
		// periodic get
		log.Debugf("target %s, starting sync: %s, Interval: %s, Paths: [ \"%s\" ]", t.name, ncc.Name, ncc.Interval.String(), strings.Join(ncc.Paths, "\", \""))
		go func(ncSync *config.SyncProtocol) {
			t.internalSync(ctx, ncSync, true, syncCh)
			ticker := time.NewTicker(ncSync.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					t.internalSync(ctx, ncSync, false, syncCh)
				}
			}
		}(ncc)
	}

	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.Canceled) {
		log.Errorf("datastore %s sync stopped: %v", t.name, ctx.Err())
	}
}

func (t *ncTarget) internalSync(ctx context.Context, sc *config.SyncProtocol, force bool, syncCh chan *SyncUpdate) {
	if t.Status().IsConnected() {
		return
	}
	// iterate syncConfig
	paths := make([]*sdcpb.Path, 0, len(sc.Paths))
	// iterate referenced paths
	for _, p := range sc.Paths {
		path, err := utils.ParsePath(p)
		if err != nil {
			log.Errorf("failed Parsing Path %q, %v", p, err)
			return
		}
		// add the parsed path
		paths = append(paths, path)
	}

	// init a DataRequest
	req := &sdcpb.GetDataRequest{
		Name:     sc.Name,
		Path:     paths,
		DataType: sdcpb.DataType_CONFIG,
		Datastore: &sdcpb.DataStore{
			Type: sdcpb.Type_MAIN,
		},
	}

	// execute netconf get
	resp, err := t.Get(ctx, req)
	if err != nil {
		log.Errorf("failed getting config: %T | %v", err, err)
		if strings.Contains(err.Error(), "EOF") {
			t.Close()
			go t.reconnect()
		}
		return
	}
	// push notifications into syncCh
	syncCh <- &SyncUpdate{
		Start: true,
		Force: force,
	}
	notificationsCount := 0
	for _, n := range resp.GetNotification() {
		syncCh <- &SyncUpdate{
			Update: n,
		}
		notificationsCount++
	}
	log.Debugf("%s: sync-ed %d notifications", t.name, notificationsCount)
	syncCh <- &SyncUpdate{
		End: true,
	}
}

func (t *ncTarget) Close() error {
	if t == nil {
		return nil
	}
	if t.driver == nil {
		return nil
	}
	return t.driver.Close()
}

func (t *ncTarget) reconnect() {
	t.m.Lock()
	defer t.m.Unlock()

	if t.Status().IsConnected() {
		return
	}

	var err error
	log.Infof("%s: NETCONF reconnecting...", t.name)
	for {
		t.driver, err = scrapligo.NewScrapligoNetconfTarget(t.sbiConfig)
		if err != nil {
			log.Errorf("failed to create NETCONF driver: %v", err)
			time.Sleep(t.sbiConfig.ConnectRetry)
			continue
		}
		log.Infof("%s: NETCONF reconnected...", t.name)
		return
	}
}

func (t *ncTarget) setRunning(source TargetSource) (*sdcpb.SetDataResponse, error) {

	xtree, err := source.ToXML(true, t.sbiConfig.NetconfOptions.IncludeNS, t.sbiConfig.NetconfOptions.OperationWithNamespace, t.sbiConfig.NetconfOptions.UseOperationRemove)
	if err != nil {
		return nil, err
	}

	xdoc, err := xtree.WriteToString()
	if err != nil {
		return nil, err
	}

	// if there was no data in the xml document, return
	if len(xdoc) == 0 {
		return &sdcpb.SetDataResponse{
			Timestamp: time.Now().UnixNano(),
		}, nil
	}

	log.Debugf("datastore %s XML:\n%s\n", t.name, xdoc)

	// edit the config
	resp, err := t.driver.EditConfig("running", xdoc)
	if err != nil {
		log.Errorf("datastore %s failed edit-config: %v", t.name, err)
		if strings.Contains(err.Error(), "EOF") {
			t.Close()
			go t.reconnect()
			return nil, err
		}
		return nil, err
	}

	// retrieve netconf rpc-error -> warnings as string array
	warnings, err := filterRPCErrors(resp.Doc, "warning")
	if err != nil {
		return nil, fmt.Errorf("filtering netconf rpc-errors with severity warnings: %w", err)
	}
	return &sdcpb.SetDataResponse{
		Warnings:  warnings,
		Timestamp: time.Now().UnixNano(),
	}, nil
}

// filterRPCErrors takes the given etree.Document, filters the document for rpc-errors with the given severity
// and returns them collectively as a []string
func filterRPCErrors(xml *etree.Document, severity string) ([]string, error) {
	var result []string
	rpcErrs := xml.FindElements(fmt.Sprintf("//rpc-error[error-severity='%s']", severity))
	for _, rpcErr := range rpcErrs {
		d := etree.NewDocumentWithRoot(rpcErr)
		s, err := d.WriteToString()
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, nil
}

func (t *ncTarget) setCandidate(source TargetSource) (*sdcpb.SetDataResponse, error) {
	xtree, err := source.ToXML(true, t.sbiConfig.NetconfOptions.IncludeNS, t.sbiConfig.NetconfOptions.OperationWithNamespace, t.sbiConfig.NetconfOptions.UseOperationRemove)
	if err != nil {
		return nil, err
	}

	xdoc, err := xtree.WriteToString()
	if err != nil {
		return nil, err
	}

	// if there was no data in the xml document, continue
	if len(xdoc) == 0 {
		return &sdcpb.SetDataResponse{
			Timestamp: time.Now().UnixNano(),
		}, nil
	}

	log.Debugf("datastore %s XML:\n%s\n", t.name, xdoc)

	// edit the config
	resp, err := t.driver.EditConfig("candidate", xdoc)
	if err != nil {
		log.Errorf("datastore %s failed edit-config: %v", t.name, err)
		if strings.Contains(err.Error(), "EOF") {
			t.Close()
			go t.reconnect()
			return nil, err
		}
		err2 := t.driver.Discard()
		if err2 != nil {
			// log failed discard
			log.Errorf("failed with %v while discarding pending changes after error %v", err2, err)
		}
		return nil, err
	}
	rpcWarnings, err := filterRPCErrors(resp.Doc, "warning")
	if err != nil {
		return nil, fmt.Errorf("filtering netconf rpc-errors with severity warnings: %w", err)
	}

	log.Infof("datastore %s: committing changes on target", t.name)
	// commit the config
	err = t.driver.Commit()
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			t.Close()
			go t.reconnect()
		}
		return nil, err
	}
	return &sdcpb.SetDataResponse{
		Warnings:  rpcWarnings,
		Timestamp: time.Now().UnixNano(),
	}, nil
}
