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

package server

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/sdcio/data-server/pkg/config"
	"github.com/sdcio/data-server/pkg/datastore"
	"github.com/sdcio/data-server/pkg/datastore/target"
)

// datastore
func (s *Server) ListDataStore(ctx context.Context, req *sdcpb.ListDataStoreRequest) (*sdcpb.ListDataStoreResponse, error) {
	log.Debug("Received ListDataStoreRequest")

	s.md.RLock()
	defer s.md.RUnlock()
	numDs := len(s.datastores)
	rs := make([]*sdcpb.GetDataStoreResponse, 0, numDs)
	for _, ds := range s.datastores {
		r, err := s.datastoreToRsp(ctx, ds)
		if err != nil {
			return nil, err
		}
		rs = append(rs, r)
	}
	return &sdcpb.ListDataStoreResponse{
		Datastores: rs,
	}, nil
}

func (s *Server) GetDataStore(ctx context.Context, req *sdcpb.GetDataStoreRequest) (*sdcpb.GetDataStoreResponse, error) {
	log.Debugf("Received GetDataStoreRequest: %v", req)
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name attribute")
	}
	s.md.RLock()
	defer s.md.RUnlock()
	ds, ok := s.datastores[name]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown datastore %s", name)
	}
	return s.datastoreToRsp(ctx, ds)
}

func (s *Server) CreateDataStore(ctx context.Context, req *sdcpb.CreateDataStoreRequest) (*sdcpb.CreateDataStoreResponse, error) {
	log.Debugf("Received CreateDataStoreRequest: %v", req)
	name := req.GetName()
	lName := len(name)
	if lName == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name attribute")
	}
	if lName > math.MaxUint16 {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name attribute")
	}
	switch {
	// create candidate
	case req.GetSchema() == nil && req.GetDatastore() != nil:
		s.md.RLock()
		defer s.md.RUnlock()
		ds, ok := s.datastores[name]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown datastore %s", name)
		}
		switch req.GetDatastore().GetType() {
		case sdcpb.Type_CANDIDATE:
			owner := req.GetDatastore().GetOwner()
			lOwner := len(owner)
			if lOwner == 0 {
				return nil, status.Error(codes.InvalidArgument, "missing owner name attribute")
			}
			if lOwner > math.MaxUint16 {
				return nil, status.Errorf(codes.InvalidArgument, "owner name too long(%d>%d)", lOwner, math.MaxUint16)
			}
			if strings.HasPrefix(owner, "__") && owner != datastore.DefaultOwner {
				return nil, status.Error(codes.InvalidArgument, "owner name cannot start with `__`")
			}
			err := ds.CreateCandidate(ctx, req.GetDatastore())
			if err != nil {
				return nil, status.Errorf(codes.Internal, "%v", err)
			}
			return &sdcpb.CreateDataStoreResponse{}, nil
		default:
			return nil, status.Errorf(codes.InvalidArgument, "schema required for MAIN datastore creation")
		}
		// create main
	case req.GetSchema() != nil:
		s.md.Lock()
		defer s.md.Unlock()
		if _, ok := s.datastores[name]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "datastore %s already exists", name)
		}

		sbi := &config.SBI{
			Type:    req.GetTarget().GetType(),
			Port:    req.GetTarget().GetPort(),
			Address: req.GetTarget().GetAddress(),
		}

		switch strings.ToLower(req.GetTarget().GetType()) {
		case "netconf":
			commitDatastore := "candidate"
			switch req.GetTarget().GetNetconfOpts().GetCommitCandidate() {
			case sdcpb.CommitCandidate_COMMIT_CANDIDATE:
			case sdcpb.CommitCandidate_COMMIT_RUNNING:
				commitDatastore = "running"
			default:
				return nil, fmt.Errorf("unknown commitDatastore: %v", req.GetTarget().GetNetconfOpts().GetCommitCandidate())
			}
			sbi.NetconfOptions = &config.SBINetconfOptions{
				IncludeNS:              req.GetTarget().GetNetconfOpts().GetIncludeNs(),
				OperationWithNamespace: req.GetTarget().GetNetconfOpts().GetOperationWithNs(),
				UseOperationRemove:     req.GetTarget().GetNetconfOpts().GetUseOperationRemove(),
				CommitDatastore:        commitDatastore,
			}

		case "gnmi":
			sbi.GnmiOptions = &config.SBIGnmiOptions{
				Encoding: req.GetTarget().GetGnmiOpts().GetEncoding(),
			}
		default:
			return nil, fmt.Errorf("unknowm protocol type %s", req.GetTarget().GetType())
		}

		if req.GetTarget().GetTls() != nil {
			sbi.TLS = &config.TLS{
				CA:         req.GetTarget().GetTls().GetCa(),
				Cert:       req.GetTarget().GetTls().GetCert(),
				Key:        req.GetTarget().GetTls().GetKey(),
				SkipVerify: req.GetTarget().GetTls().GetSkipVerify(),
			}
		}
		if req.GetTarget().GetCredentials() != nil {
			sbi.Credentials = &config.Creds{
				Username: req.GetTarget().GetCredentials().GetUsername(),
				Password: req.GetTarget().GetCredentials().GetPassword(),
				Token:    req.GetTarget().GetCredentials().GetToken(),
			}
		}

		dsConfig := &config.DatastoreConfig{
			Name: name,
			Schema: &config.SchemaConfig{
				Name:    req.GetSchema().GetName(),
				Vendor:  req.GetSchema().GetVendor(),
				Version: req.GetSchema().GetVersion(),
			},
			SBI: sbi,
		}
		if req.GetSync() != nil {
			dsConfig.Sync = &config.Sync{
				Validate:     req.GetSync().GetValidate(),
				Buffer:       req.GetSync().GetBuffer(),
				WriteWorkers: req.GetSync().GetWriteWorkers(),
				Config:       make([]*config.SyncProtocol, 0, len(req.GetSync().GetConfig())),
			}
			for _, pSync := range req.GetSync().GetConfig() {
				gnSyncConfig := &config.SyncProtocol{
					Protocol: pSync.GetTarget().GetType(),
					Name:     pSync.GetName(),
					Paths:    pSync.GetPath(),
					Interval: time.Duration(pSync.GetInterval()),
				}
				switch pSync.GetTarget().GetType() {
				case "gnmi":
					gnSyncConfig.Mode = "on-change"
					switch pSync.GetMode() {
					case sdcpb.SyncMode_SM_ON_CHANGE:
					case sdcpb.SyncMode_SM_SAMPLE:
						gnSyncConfig.Mode = "sample"
					case sdcpb.SyncMode_SM_ONCE:
						gnSyncConfig.Mode = "once"
					case sdcpb.SyncMode_SM_GET:
						gnSyncConfig.Mode = "get"
					}
					gnSyncConfig.Encoding = pSync.GetTarget().GetGnmiOpts().GetEncoding()
				case "netconf":
				default:
					return nil, status.Errorf(codes.InvalidArgument, "unknown sync protocol: %q", pSync.GetTarget().GetType())
				}
				dsConfig.Sync.Config = append(dsConfig.Sync.Config, gnSyncConfig)
			}
		}
		err := dsConfig.ValidateSetDefaults()
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid datastore config: %v", err)
		}
		s.datastores[req.GetName()] = datastore.New(
			s.ctx,
			dsConfig,
			s.schemaClient,
			s.cacheClient,
			s.gnmiOpts...)
		return &sdcpb.CreateDataStoreResponse{}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "schema or datastore must be set")
	}
}

func (s *Server) DeleteDataStore(ctx context.Context, req *sdcpb.DeleteDataStoreRequest) (*sdcpb.DeleteDataStoreResponse, error) {
	log.Debugf("Received DeleteDataStoreRequest: %v", req)
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name attribute")
	}
	s.md.Lock()
	defer s.md.Unlock()
	ds, ok := s.datastores[name]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown datastore %s", name)
	}

	switch {
	case req.GetDatastore() == nil:
		err := ds.Stop()
		if err != nil {
			log.Errorf("failed to stop datastore %s: %v", name, err)
		}
		err = ds.DeleteCache(ctx)
		if err != nil {
			log.Errorf("failed to delete the datastore %s cache: %v", name, err)
		}
		delete(s.datastores, name)
		log.Infof("deleted datastore %s", name)
		return &sdcpb.DeleteDataStoreResponse{}, nil
	default:
		switch req.GetDatastore().GetType() {
		case *sdcpb.Type_CANDIDATE.Enum():
			ds.DeleteCandidate(ctx, req.GetDatastore().GetName())
			log.Infof("datastore %s deleted candidate %s", name, req.GetDatastore().GetName())
		case *sdcpb.Type_MAIN.Enum():
			err := ds.Stop()
			if err != nil {
				log.Errorf("failed to stop datastore %s: %v", name, err)
			}
			delete(s.datastores, name)
			log.Infof("deleted datastore %s", name)
		}
		return &sdcpb.DeleteDataStoreResponse{}, nil
	}
}

func (s *Server) Discard(ctx context.Context, req *sdcpb.DiscardRequest) (*sdcpb.DiscardResponse, error) {
	log.Debugf("Received DiscardDataStoreRequest: %v", req)
	name := req.GetName()
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "missing name attribute")
	}
	s.md.RLock()
	defer s.md.RUnlock()
	ds, ok := s.datastores[name]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown datastore %s", name)
	}
	err := ds.Discard(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &sdcpb.DiscardResponse{}, nil
}

func (s *Server) WatchDeviations(req *sdcpb.WatchDeviationRequest, stream sdcpb.DataServer_WatchDeviationsServer) error {
	log.Debugf("Received WatchDeviationRequest: %v", req)
	ctx := stream.Context()
	peerInfo, ok := peer.FromContext(ctx)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "missing peer info")
	}
	if req.GetName() == nil {
		return status.Errorf(codes.InvalidArgument, "missing datastore name")
	}
	s.md.RLock()
	ds, ok := s.datastores[req.GetName()[0]]
	s.md.RUnlock()
	if !ok {
		return status.Errorf(codes.InvalidArgument, "unknown datastore name: %s", req.GetName()[0])
	}
	_ = ds.WatchDeviations(req, stream)
	<-stream.Context().Done()
	ds.StopDeviationsWatch(peerInfo.Addr.String())
	return nil
}

func (s *Server) datastoreToRsp(ctx context.Context, ds *datastore.Datastore) (*sdcpb.GetDataStoreResponse, error) {
	cands, err := ds.Candidates(ctx)
	if err != nil {
		return nil, err
	}
	rsp := &sdcpb.GetDataStoreResponse{
		Name:      ds.Config().Name,
		Datastore: make([]*sdcpb.DataStore, 0, len(cands)+1),
	}
	rsp.Datastore = append(rsp.Datastore,
		&sdcpb.DataStore{
			Type: *sdcpb.Type_MAIN.Enum().Enum(),
			Name: ds.Config().Name,
		},
	)
	rsp.Datastore = append(rsp.Datastore, cands...)
	rsp.Target = &sdcpb.Target{
		Type:    ds.Config().SBI.Type,
		Address: ds.Config().SBI.Address,
	}
	// map datastore sbi conn state to sdcpb.TargetStatus
	switch ds.ConnectionState().Status {
	case target.TargetStatusConnected:
		rsp.Target.Status = sdcpb.TargetStatus_CONNECTED
	case target.TargetStatusNotConnected:
		rsp.Target.Status = sdcpb.TargetStatus_NOT_CONNECTED
	default:
		rsp.Target.Status = sdcpb.TargetStatus_UNKNOWN
	}

	rsp.Schema = ds.Config().Schema.GetSchema()
	return rsp, nil
}
