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
	"strconv"
	"strings"
	"time"

	"github.com/sdcio/cache/proto/cachepb"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/utils"
)

var rawIntentPrefix = "__raw_intent__"

const (
	intentRawNameSep = "_"
)

var ErrIntentNotFound = errors.New("intent not found")

func (d *Datastore) GetIntent(ctx context.Context, req *sdcpb.GetIntentRequest) (*sdcpb.GetIntentResponse, error) {
	r, err := d.getRawIntent(ctx, req.GetIntent(), req.GetPriority())
	if err != nil {
		return nil, err
	}

	rsp := &sdcpb.GetIntentResponse{
		Name: d.Name(),
		Intent: &sdcpb.Intent{
			Intent:   r.GetIntent(),
			Priority: r.GetPriority(),
			Update:   r.GetUpdate(),
		},
	}
	return rsp, nil
}

func (d *Datastore) SetIntent(ctx context.Context, req *sdcpb.SetIntentRequest) (*sdcpb.SetIntentResponse, error) {
	if !d.intentMutex.TryLock() {
		return nil, status.Errorf(codes.ResourceExhausted, "datastore %s has an ongoing SetIntentRequest", d.Name())
	}
	defer d.intentMutex.Unlock()

	log.Infof("received SetIntentRequest: ds=%s intent=%s", req.GetName(), req.GetIntent())
	now := time.Now().UnixNano()
	candidateName := fmt.Sprintf("%s-%d", req.GetIntent(), now)
	err := d.CreateCandidate(ctx, &sdcpb.DataStore{
		Type:     sdcpb.Type_CANDIDATE,
		Name:     candidateName,
		Owner:    req.GetIntent(),
		Priority: req.GetPriority(),
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		// delete candidate
		err := d.cacheClient.DeleteCandidate(ctx, d.Name(), candidateName)
		if err != nil {
			log.Errorf("%s: failed to delete candidate %s: %v", d.Name(), candidateName, err)
		}
	}()
	switch {
	case len(req.GetUpdate()) > 0:
		err = d.SetIntentUpdate(ctx, req, candidateName)
		if err != nil {
			log.Errorf("%s: failed to SetIntentUpdate: %v", d.Name(), err)
			return nil, err
		}
	case req.GetDelete():
		err = d.SetIntentDelete(ctx, req, candidateName)
		if err != nil {
			log.Errorf("%s: failed to SetIntentDelete: %v", d.Name(), err)
			return nil, err
		}
	}

	return &sdcpb.SetIntentResponse{}, nil
}

func (d *Datastore) ListIntent(ctx context.Context, req *sdcpb.ListIntentRequest) (*sdcpb.ListIntentResponse, error) {
	intents, err := d.listRawIntent(ctx)
	if err != nil {
		return nil, err
	}
	return &sdcpb.ListIntentResponse{
		Name:   req.GetName(),
		Intent: intents,
	}, nil
}

func (d *Datastore) getIntentFlatNotifications(ctx context.Context, intentName string, priority int32) ([]*sdcpb.Notification, error) {
	notifications := make([]*sdcpb.Notification, 0)

	rawIntent, err := d.getRawIntent(ctx, intentName, priority)
	if errors.Is(err, ErrIntentNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	expUpds, err := d.expandUpdates(ctx, rawIntent.GetUpdate(), true)
	if err != nil {
		return nil, err
	}
	paths := make([][]string, 0, len(expUpds))
	for _, expu := range expUpds {
		paths = append(paths, utils.ToStrings(expu.Path, false, false))
	}
	upds := d.cacheClient.Read(ctx, d.config.Name, &cache.Opts{
		Store:    cachepb.Store_INTENDED,
		Owner:    intentName,
		Priority: priority,
	}, paths, 0)

	for _, upd := range upds {
		if upd.Owner() != intentName {
			continue // TODO: DIRTY temp(?) workaround for 2 intents with the same priority
		}
		scp, err := d.toPath(ctx, upd.GetPath())
		if err != nil {
			return nil, err
		}
		tv, err := upd.Value()
		if err != nil {
			return nil, err
		}
		n := &sdcpb.Notification{
			Timestamp: time.Now().UnixNano(),
			Update: []*sdcpb.Update{{
				Path:  scp,
				Value: tv,
			}},
		}
		notifications = append(notifications, n)
	}
	log.Debug()
	log.Debugf("ds=%s | %s | current notifications: %v", d.Name(), intentName, notifications)
	log.Debug()
	return notifications, nil
}

func (d *Datastore) applyIntent(ctx context.Context, candidateName string, priority int32, sdreq *sdcpb.SetDataRequest) error {
	if candidateName == "" {
		return fmt.Errorf("missing candidate name")
	}
	log.Debugf("%s: applying intent from candidate %s", d.Name(), sdreq.GetDatastore())

	var err error
	sbiSet := &sdcpb.SetDataRequest{
		Update: []*sdcpb.Update{},
		Delete: []*sdcpb.Path{},
	}

	log.Debugf("%s: %s notification:\n%s", d.Name(), candidateName, prototext.Format(sdreq))
	// TODO: consider if leafref validation
	// needs to run before must statements validation
	// validate MUST statements
	log.Infof("%s: validating must statements candidate %s", d.Name(), sdreq.GetDatastore())
	for _, upd := range sdreq.GetUpdate() {
		log.Debugf("%s: %s validating must statement on: %v", d.Name(), candidateName, upd)
		_, err = d.validateMustStatement(ctx, candidateName, upd.GetPath(), false)
		if err != nil {
			return err
		}
	}
	log.Infof("%s: validating leafrefs candidate %s", d.Name(), sdreq.GetDatastore())
	for _, upd := range sdreq.GetUpdate() {
		log.Debugf("%s: %s validating leafRef on update: %v", d.Name(), candidateName, upd)
		err = d.validateLeafRef(ctx, upd, candidateName)
		if err != nil {
			return err
		}
	}

	// push updates to sbi
	sbiSet = &sdcpb.SetDataRequest{
		Update: sdreq.GetUpdate(),
		Delete: sdreq.GetDelete(),
	}
	log.Debugf("datastore %s/%s applyIntent:\n%s", d.config.Name, candidateName, prototext.Format(sbiSet))

	log.Infof("datastore %s/%s applyIntent: sending a setDataRequest with num_updates=%d, num_replaces=%d, num_deletes=%d",
		d.config.Name, candidateName, len(sbiSet.GetUpdate()), len(sbiSet.GetReplace()), len(sbiSet.GetDelete()))

	// send set request only if there are updates and/or deletes
	if len(sbiSet.GetUpdate())+len(sbiSet.GetReplace())+len(sbiSet.GetDelete()) > 0 {
		rsp, err := d.sbi.Set(ctx, sbiSet)
		if err != nil {
			return err
		}
		log.Debugf("datastore %s/%s SetResponse from SBI: %v", d.config.Name, candidateName, rsp)
	}

	return nil
}

func (d *Datastore) validateChoiceCases(ctx context.Context, updates []*sdcpb.Update, replaces []*sdcpb.Update, candidateName string) error {
	_ = replaces
	scb := d.getValidationClient().SchemaClientBound
	ccb := d.getValidationClient().CacheClientBound

	for _, u := range updates {
		done := make(chan struct{})
		schemaElemChan, err := scb.GetSchemaElements(ctx, u.GetPath(), done)
		if err != nil {
			return err
		}

		schemaElemIndex := -1
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sch, ok := <-schemaElemChan:
				schemaElemIndex++
				if !ok {
					return nil
				}
				var choiceInfo *sdcpb.ChoiceInfo
				switch sch.GetSchema().GetSchema().(type) {
				case *sdcpb.SchemaElem_Container:
					choiceInfo = sch.GetSchema().GetContainer().GetChoiceInfo()
				case *sdcpb.SchemaElem_Field:
					choiceInfo = sch.GetSchema().GetField().GetChoiceInfo()
				case *sdcpb.SchemaElem_Leaflist:
					choiceInfo = sch.GetSchema().GetContainer().GetChoiceInfo()
				}
				if choiceInfo != nil {
					// build the path up to the given choice-info
					p := &sdcpb.Path{
						Origin: u.GetPath().GetOrigin(),
						Target: u.GetPath().GetTarget(),
						Elem:   u.GetPath().GetElem()[:schemaElemIndex+1],
					}
					log.Infof("CHOICE-INFO: %s on %s", choiceInfo.String(), p.String())

					tv, err := ccb.GetValues(ctx, candidateName, p)
					if err != nil {
						return err
					}
					_ = tv
				}
			}
		}
	}

	// tv, err := ccb.GetValue(ctx, "default", &sdcpb.Path{Elem: []*sdcpb.PathElem{{Name: "interface", Key: map[string]string{"name": "ethernet-0/1"}}}})
	// if err != nil {
	// 	return err
	// }
	// _ = tv

	// done := make(chan struct{})
	// for _, u := range updates {
	// 	elemChan, err := scb.GetSchemaElements(ctx, u.GetPath(), done)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			return ctx.Err()
	// 		case sch, ok := <-elemChan:
	// 			if !ok {
	// 				return nil
	// 			}
	// 			_ = sch

	// 		}
	// 	}
	// 	close(done)
	// }

	// _ = scb
	return nil
}

func (d *Datastore) saveRawIntent(ctx context.Context, intentName string, req *sdcpb.SetIntentRequest) error {
	b, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	//
	rin := rawIntentName(intentName, req.GetPriority())
	upd, err := d.cacheClient.NewUpdate(
		&sdcpb.Update{
			Path: &sdcpb.Path{
				Elem: []*sdcpb.PathElem{{Name: rin}},
			},
			Value: &sdcpb.TypedValue{
				Value: &sdcpb.TypedValue_BytesVal{BytesVal: b},
			},
		},
	)
	if err != nil {
		return err
	}
	err = d.cacheClient.Modify(ctx, d.config.Name,
		&cache.Opts{
			Store: cachepb.Store_INTENTS,
		},
		nil,
		[]*cache.Update{upd})
	if err != nil {
		return err
	}
	return nil
}

func (d *Datastore) getRawIntent(ctx context.Context, intentName string, priority int32) (*sdcpb.SetIntentRequest, error) {
	rin := rawIntentName(intentName, priority)
	upds := d.cacheClient.Read(ctx, d.config.Name, &cache.Opts{
		Store: cachepb.Store_INTENTS,
	}, [][]string{{rin}}, 0)
	if len(upds) == 0 {
		return nil, ErrIntentNotFound
	}

	val, err := upds[0].Value()
	if err != nil {
		return nil, err
	}
	req := &sdcpb.SetIntentRequest{}
	err = proto.Unmarshal(val.GetBytesVal(), req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (d *Datastore) listRawIntent(ctx context.Context) ([]*sdcpb.Intent, error) {
	upds := d.cacheClient.Read(ctx, d.config.Name, &cache.Opts{
		Store:    cachepb.Store_INTENTS,
		KeysOnly: true,
	}, [][]string{{"*"}}, 0)
	numUpds := len(upds)
	if numUpds == 0 {
		return nil, nil
	}
	intents := make([]*sdcpb.Intent, 0, numUpds)
	for _, upd := range upds {
		if len(upd.GetPath()) == 0 {
			return nil, fmt.Errorf("malformed raw intent name: %q", upd.GetPath()[0])
		}
		intentRawName := strings.TrimPrefix(upd.GetPath()[0], rawIntentPrefix)
		intentNameComp := strings.Split(intentRawName, intentRawNameSep)
		inc := len(intentNameComp)
		if inc < 2 {
			return nil, fmt.Errorf("malformed raw intent name: %q", upd.GetPath()[0])
		}
		pr, err := strconv.Atoi(intentNameComp[inc-1])
		if err != nil {
			return nil, fmt.Errorf("malformed raw intent name: %q: %v", upd.GetPath()[0], err)
		}
		in := &sdcpb.Intent{
			Intent:   strings.Join(intentNameComp[:inc-1], intentRawNameSep),
			Priority: int32(pr),
		}
		intents = append(intents, in)
	}
	sort.Slice(intents, func(i, j int) bool {
		if intents[i].GetPriority() == intents[j].GetPriority() {
			return intents[i].GetIntent() < intents[j].GetIntent()
		}
		return intents[i].GetPriority() < intents[j].GetPriority()
	})
	return intents, nil
}

func (d *Datastore) deleteRawIntent(ctx context.Context, intentName string, priority int32) error {
	return d.cacheClient.Modify(ctx, d.config.Name,
		&cache.Opts{
			Store: cachepb.Store_INTENTS,
		},
		[][]string{{rawIntentName(intentName, priority)}},
		nil)
}

func (d *Datastore) pathsAddKeysAsLeaves(paths []*sdcpb.Path) []*sdcpb.Path {
	added := make(map[string]struct{})
	npaths := make([]*sdcpb.Path, 0, len(paths))
	for _, p := range paths {
		npaths = append(npaths, p)

		for idx, pe := range p.GetElem() {
			if len(pe.GetKey()) == 0 {
				continue
			}
			for k, v := range pe.GetKey() {
				pp := &sdcpb.Path{
					Elem: make([]*sdcpb.PathElem, idx+1),
				}
				for i := 0; i < idx+1; i++ {
					pp.Elem[i] = &sdcpb.PathElem{
						Name: p.GetElem()[i].GetName(),
						Key:  utils.CopyMap(p.GetElem()[i].GetKey()),
					}
				}
				pp.Elem = append(pp.Elem, &sdcpb.PathElem{Name: k})

				uniqueID := utils.ToXPath(pp, false) + ":::" + v
				if _, ok := added[uniqueID]; !ok {
					added[uniqueID] = struct{}{}
					npaths = append(npaths, pp)
				}
			}
		}
		// fmt.Println()
	}
	return npaths
}

func (d *Datastore) buildPathsWithKeysAsLeaves(paths []*sdcpb.Path) []*sdcpb.Path {
	added := make(map[string]struct{})
	npaths := make([]*sdcpb.Path, 0, len(paths))
	for _, p := range paths {
		for idx, pe := range p.GetElem() {
			if len(pe.GetKey()) == 0 {
				continue
			}
			for k, v := range pe.GetKey() {
				pp := &sdcpb.Path{
					Elem: make([]*sdcpb.PathElem, idx+1),
				}
				for i := 0; i < idx+1; i++ {
					pp.Elem[i] = &sdcpb.PathElem{
						Name: p.GetElem()[i].GetName(),
						Key:  utils.CopyMap(p.GetElem()[i].GetKey()),
					}
				}
				pp.Elem = append(pp.Elem, &sdcpb.PathElem{Name: k})

				uniqueID := utils.ToXPath(pp, false) + ":::" + v
				if _, ok := added[uniqueID]; !ok {
					// fmt.Printf("d | ADDING KEY Path: %v\n", pp)
					added[uniqueID] = struct{}{}
					npaths = append(npaths, pp)
				}
			}
		}
		// fmt.Println()
	}
	return npaths
}

func (d *Datastore) cacheUpdateToUpdate(ctx context.Context, cupd *cache.Update) (*sdcpb.Update, error) {
	scp, err := d.toPath(ctx, cupd.GetPath())
	if err != nil {
		return nil, err
	}
	val, err := cupd.Value()
	if err != nil {
		return nil, err
	}
	return &sdcpb.Update{
		Path:  scp,
		Value: val,
	}, nil
}

func rawIntentName(name string, pr int32) string {
	return fmt.Sprintf("%s%s%s%d", rawIntentPrefix, name, intentRawNameSep, pr)
}
