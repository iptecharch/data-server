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
	"strings"
	"sync"
	"time"

	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// data
func (s *Server) GetData(req *sdcpb.GetDataRequest, stream sdcpb.DataServer_GetDataServer) error {
	pr, _ := peer.FromContext(stream.Context())
	log.Debugf("received GetData request %v from peer %s", req, pr.Addr.String())
	name := req.GetName()
	if name == "" {
		return status.Error(codes.InvalidArgument, "missing datastore name")
	}
	switch req.GetDataType() {
	case sdcpb.DataType_STATE:
		if req.GetDatastore().GetType() == sdcpb.Type_CANDIDATE {
			return status.Error(codes.InvalidArgument, "a candidate datastore does not store state data")
		}
	}
	if len(req.GetPath()) == 0 {
		return status.Error(codes.InvalidArgument, "missing path attribute")
	}

	s.md.RLock()
	defer s.md.RUnlock()
	ds, ok := s.datastores[name]
	if !ok {
		return status.Errorf(codes.InvalidArgument, "unknown datastore %s", name)
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	nCh := make(chan *sdcpb.GetDataResponse)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stream.Context().Done():
				return
			case rsp, ok := <-nCh:
				if !ok {
					return
				}
				err := stream.Send(rsp)
				if err != nil {
					if strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "EOF") {
						return
					}
					log.Errorf("GetData stream send err :%v", err)
				}
			}
		}
	}()
	err := ds.Get(stream.Context(), req, nCh)
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

func (s *Server) Subscribe(req *sdcpb.SubscribeRequest, stream sdcpb.DataServer_SubscribeServer) error {
	log.Infof("received SubscribeRequest: %v", req)
	name := req.GetName()
	if name == "" {
		return status.Errorf(codes.InvalidArgument, "missing datastore name")
	}

	if len(req.GetSubscription()) == 0 {
		return status.Errorf(codes.InvalidArgument, "missing subscription list in request")
	}
	// TODO: set subscribe request defaults
	for _, subsc := range req.GetSubscription() {
		if subsc.GetSampleInterval() < uint64(time.Second) {
			subsc.SampleInterval = uint64(time.Second)
		}
	}

	s.md.RLock()
	ds, ok := s.datastores[name]
	s.md.RUnlock()
	if !ok {
		return status.Errorf(codes.InvalidArgument, "unknown datastore %s", name)
	}
	return ds.Subscribe(req, stream)
}

func (s *Server) Watch(req *sdcpb.WatchRequest, stream sdcpb.DataServer_WatchServer) error {
	return nil
}
