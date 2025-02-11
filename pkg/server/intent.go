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

	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func (s *Server) GetIntent(ctx context.Context, req *sdcpb.GetIntentRequest) (*sdcpb.GetIntentResponse, error) {
	pr, _ := peer.FromContext(ctx)
	log.Debugf("received GetIntent request %v from peer %s", req, pr.Addr.String())

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name")
	}
	if req.GetIntent() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing intent name")
	}
	if req.GetPriority() == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing intent priority")
	}
	ds, err := s.getDataStore(req.Name)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return ds.GetIntent(ctx, req)
}

// func (s *Server) SetIntent(ctx context.Context, req *sdcpb.SetIntentRequest) (*sdcpb.SetIntentResponse, error) {
// 	pr, _ := peer.FromContext(ctx)
// 	log.Debugf("received SetIntent request %v from peer %s", req, pr.Addr.String())

// 	if req.GetName() == "" {
// 		return nil, status.Error(codes.InvalidArgument, "missing datastore name")
// 	}
// 	if req.GetIntent() == "" {
// 		return nil, status.Error(codes.InvalidArgument, "missing intent name")
// 	}
// 	if len(req.GetUpdate()) == 0 && !req.GetDelete() {
// 		return nil, status.Error(codes.InvalidArgument, "updates or a delete flag must be set")
// 	}
// 	if len(req.GetUpdate()) != 0 && req.GetDelete() {
// 		return nil, status.Error(codes.InvalidArgument, "both updates and the delete flag cannot be set at the same time")
// 	}
// 	ds, err := s.getDataStore(req.Name)
// 	if err != nil {
// 		return nil, status.Error(codes.NotFound, err.Error())
// 	}
// 	return ds.SetIntent(ctx, req)
// }

func (s *Server) ListIntent(ctx context.Context, req *sdcpb.ListIntentRequest) (*sdcpb.ListIntentResponse, error) {
	pr, _ := peer.FromContext(ctx)
	log.Debugf("received ListIntent request %v from peer %s", req, pr.Addr.String())

	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name")
	}
	ds, err := s.getDataStore(req.Name)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return ds.ListIntent(ctx, req)
}
