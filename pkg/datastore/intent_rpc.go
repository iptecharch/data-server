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

	"github.com/sdcio/cache/proto/cachepb"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"

	"github.com/sdcio/data-server/pkg/cache"
	"github.com/sdcio/data-server/pkg/datastore/target"
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

// func (d *Datastore) SetIntent(ctx context.Context, req *sdcpb.SetIntentRequest) (*sdcpb.SetIntentResponse, error) {
// 	if !d.intentMutex.TryLock() {
// 		return nil, status.Errorf(codes.ResourceExhausted, "datastore %s has an ongoing SetIntentRequest", d.Name())
// 	}
// 	defer d.intentMutex.Unlock()

// 	log.Infof("received SetIntentRequest: ds=%s intent=%s", req.GetName(), req.GetIntent())
// 	now := time.Now().UnixNano()
// 	candidateName := fmt.Sprintf("%s-%d", req.GetIntent(), now)
// 	err := d.CreateCandidate(ctx, &sdcpb.DataStore{
// 		Type:     sdcpb.Type_CANDIDATE,
// 		Name:     candidateName,
// 		Owner:    req.GetIntent(),
// 		Priority: req.GetPriority(),
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer func() {
// 		// delete candidate
// 		err := d.cacheClient.DeleteCandidate(ctx, d.Name(), candidateName)
// 		if err != nil {
// 			log.Errorf("%s: failed to delete candidate %s: %v", d.Name(), candidateName, err)
// 		}
// 	}()

// 	setIntentResponse, err := d.SetIntentUpdate(ctx, req, candidateName)
// 	if err != nil {
// 		log.Errorf("%s: failed to SetIntentUpdate: %v", d.Name(), err)
// 		return nil, err
// 	}

// 	return setIntentResponse, nil
// }

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

func (d *Datastore) applyIntent(ctx context.Context, source target.TargetSource) (*sdcpb.SetDataResponse, error) {
	var err error

	var rsp *sdcpb.SetDataResponse
	// send set request only if there are updates and/or deletes

	if d.sbi == nil {
		return nil, fmt.Errorf("%s is not connected", d.config.Name)
	}

	rsp, err = d.sbi.Set(ctx, source)
	if err != nil {
		return nil, err
	}
	log.Debugf("datastore %s SetResponse from SBI: %v", d.config.Name, rsp)

	return rsp, nil
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

func rawIntentName(name string, pr int32) string {
	return fmt.Sprintf("%s%s%s%d", rawIntentPrefix, name, intentRawNameSep, pr)
}
