package target

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	sdcpb "github.com/iptecharch/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"

	"github.com/iptecharch/data-server/pkg/config"
	"github.com/iptecharch/data-server/pkg/datastore/target/netconf"
	"github.com/iptecharch/data-server/pkg/datastore/target/netconf/driver/scrapligo"
	"github.com/iptecharch/data-server/pkg/schema"
	"github.com/iptecharch/data-server/pkg/utils"
)

type ncTarget struct {
	name   string
	driver netconf.Driver

	m         *sync.Mutex
	connected bool

	schemaClient schema.Client
	schema       *sdcpb.Schema
	sbiConfig    *config.SBI
}

func newNCTarget(_ context.Context, name string, cfg *config.SBI, schemaClient schema.Client, schema *sdcpb.Schema) (*ncTarget, error) {
	t := &ncTarget{
		name:         name,
		m:            new(sync.Mutex),
		connected:    false,
		schemaClient: schemaClient,
		schema:       schema,
		sbiConfig:    cfg,
	}
	var err error
	// create a new NETCONF driver
	t.driver, err = scrapligo.NewScrapligoNetconfTarget(cfg)
	if err != nil {
		return t, err
	}
	t.connected = true
	return t, nil

}

func (t *ncTarget) Get(ctx context.Context, req *sdcpb.GetDataRequest) (*sdcpb.GetDataResponse, error) {
	if !t.connected {
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
	pathfilterXmlBuilder := netconf.NewXMLConfigBuilder(t.schemaClient, t.schema,
		&netconf.XMLConfigBuilderOpts{
			HonorNamespace:         t.sbiConfig.IncludeNS,
			OperationWithNamespace: t.sbiConfig.OperationWithNamespace,
			UseOperationRemove:     t.sbiConfig.UseOperationRemove,
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
			t.connected = false
			go t.reconnect()
		}
		return nil, err
	}

	log.Debugf("netconf response:\n%s", ncResponse.DocAsString())

	// init an XML2sdcpbConfigAdapter used to convert the netconf xml config to a sdcpb.Notification
	data := netconf.NewXML2sdcpbConfigAdapter(t.schemaClient, t.schema)

	// start transformation, which yields the sdcpb_Notification
	noti, err := data.Transform(ctx, ncResponse.Doc)
	if err != nil {
		return nil, err
	}

	// building the resulting sdcpb.GetDataResponse struct
	result := &sdcpb.GetDataResponse{
		Notification: []*sdcpb.Notification{
			noti,
		},
	}
	return result, nil
}

func (t *ncTarget) Set(ctx context.Context, req *sdcpb.SetDataRequest) (*sdcpb.SetDataResponse, error) {
	if !t.connected {
		return nil, fmt.Errorf("not connected")
	}
	xcbCfg := &netconf.XMLConfigBuilderOpts{
		HonorNamespace:         t.sbiConfig.IncludeNS,
		OperationWithNamespace: t.sbiConfig.OperationWithNamespace,
		UseOperationRemove:     t.sbiConfig.UseOperationRemove,
	}
	xmlCBDelete := netconf.NewXMLConfigBuilder(t.schemaClient, t.schema, xcbCfg)

	// iterate over the delete array
	for _, p := range req.GetDelete() {
		xmlCBDelete.Delete(ctx, p)
	}

	// iterate over the replace array
	// ATTENTION: This is not implemented intentionally, since it is expected,
	//  	that the datastore will only come up with deletes and updates.
	// 		actual replaces will be resolved to deletes and updates by the datastore
	// 		also replaces would only really make sense with jsonIETF encoding, where
	// 		an entire branch is replaces, on single values this is covered via an
	// 		update.
	//
	// for _, r := range req.Replace {
	// }
	//

	xmlCBAdd := netconf.NewXMLConfigBuilder(t.schemaClient, t.schema, xcbCfg)

	// iterate over the update array
	for _, u := range req.Update {
		xmlCBAdd.AddValue(ctx, u.Path, u.Value)
	}

	// first apply the deletes before the adds
	for _, xml := range []*netconf.XMLConfigBuilder{xmlCBDelete, xmlCBAdd} {
		// finally retrieve the xml config as string
		xdoc, err := xml.GetDoc()
		if err != nil {
			return nil, err
		}

		// if there was no data in the xml document, continue
		if len(xdoc) == 0 {
			continue
		}

		log.Debugf("datastore %s XML:\n%s\n", t.name, xdoc)

		// edit the config
		_, err = t.driver.EditConfig("candidate", xdoc)
		if err != nil {
			log.Errorf("datastore %s failed edit-config: %v", t.name, err)
			if strings.Contains(err.Error(), "EOF") {
				t.Close()
				t.connected = false
				go t.reconnect()
				return nil, err
			}
			err2 := t.driver.Discard()
			if err != nil {
				// log failed discard
				log.Errorf("failed with %v while discarding pending changes after error %v", err2, err)
			}
			return nil, err
		}

	}
	log.Infof("datastore %s: committing changes on target", t.name)
	// commit the config
	err := t.driver.Commit()
	if err != nil {
		if strings.Contains(err.Error(), "EOF") {
			t.Close()
			t.connected = false
			go t.reconnect()
		}
		return nil, err
	}
	return &sdcpb.SetDataResponse{
		Timestamp: time.Now().UnixNano(),
	}, nil
}

func (t *ncTarget) Status() string {
	if t == nil || t.driver == nil {
		return "NOT_CONNECTED"
	}
	if t.driver.IsAlive() {
		return "CONNECTED"
	}
	return "NOT_CONNECTED"
}

func (t *ncTarget) Sync(ctx context.Context, syncConfig *config.Sync, syncCh chan *SyncUpdate) {
	log.Infof("starting target %s sync", t.sbiConfig.Address)

	for _, ncc := range syncConfig.Config {
		// periodic get
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
	if !t.connected {
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
			t.connected = false
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

	if t.connected {
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
		t.connected = true
		return
	}
}
