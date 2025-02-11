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
	"fmt"

	"github.com/beevik/etree"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"google.golang.org/grpc"

	"github.com/sdcio/data-server/pkg/config"
	schemaClient "github.com/sdcio/data-server/pkg/datastore/clients/schema"
)

const (
	targetTypeNOOP    = "noop"
	targetTypeNETCONF = "netconf"
	targetTypeGNMI    = "gnmi"
)

type TargetStatus struct {
	Status  TargetConnectionStatus
	Details string
}

func NewTargetStatus(status TargetConnectionStatus) *TargetStatus {
	return &TargetStatus{
		Status: status,
	}
}
func (ts *TargetStatus) IsConnected() bool {
	return ts.Status == TargetStatusConnected
}

type TargetConnectionStatus string

const (
	TargetStatusConnected    TargetConnectionStatus = "connected"
	TargetStatusNotConnected TargetConnectionStatus = "not connected"
)

type Target interface {
	Get(ctx context.Context, req *sdcpb.GetDataRequest) (*sdcpb.GetDataResponse, error)
	Set(ctx context.Context, source TargetSource) (*sdcpb.SetDataResponse, error)
	Sync(ctx context.Context, syncConfig *config.Sync, syncCh chan *SyncUpdate)
	Status() *TargetStatus
	Close() error
}

func New(ctx context.Context, name string, cfg *config.SBI, schemaClient schemaClient.SchemaClientBound, opts ...grpc.DialOption) (Target, error) {
	switch cfg.Type {
	case targetTypeGNMI:
		return newGNMITarget(ctx, name, cfg, opts...)
	case targetTypeNETCONF:
		return newNCTarget(ctx, name, cfg, schemaClient)
	case targetTypeNOOP, "":
		return newNoopTarget(ctx, name)
	}
	return nil, fmt.Errorf("unknown DS target type %q", cfg.Type)
}

type SyncUpdate struct {
	// identifies the store this updates needs to be written to if Sync.Validate == false
	Store string
	// The received update
	Update *sdcpb.Notification
	// if true indicates the start of cache pruning
	Start bool
	// if true and start is true indicates first sync iteration,
	// it overrides any ongoing pruning in the cache.
	Force bool
	// if true indicates the end of a sync iteration.
	// triggers the pruning on the cache side.
	End bool
}

type TargetSource interface {
	// ToJson returns the Tree contained structure as JSON
	// use e.g. json.MarshalIndent() on the returned struct
	ToJson(onlyNewOrUpdated bool) (any, error)
	// ToJsonIETF returns the Tree contained structure as JSON_IETF
	// use e.g. json.MarshalIndent() on the returned struct
	ToJsonIETF(onlyNewOrUpdated bool) (any, error)
	ToXML(onlyNewOrUpdated bool, honorNamespace bool, operationWithNamespace bool, useOperationRemove bool) (*etree.Document, error)
	ToProtoUpdates(ctx context.Context, onlyNewOrUpdated bool) ([]*sdcpb.Update, error)
	ToProtoDeletes(ctx context.Context) ([]*sdcpb.Path, error)
}
