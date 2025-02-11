package server

import (
	"context"
	"errors"
	"time"

	"github.com/sdcio/data-server/pkg/datastore"
	"github.com/sdcio/data-server/pkg/datastore/types"
	"github.com/sdcio/data-server/pkg/tree"
	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func (s *Server) TransactionSet(ctx context.Context, req *sdcpb.TransactionSetRequest) (*sdcpb.TransactionSetResponse, error) {
	pr, _ := peer.FromContext(ctx)
	log.Debugf("received TransactionSetRequest request %v from peer %s", req, pr.Addr.String())

	if req.GetDatastoreName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name")
	}

	// load either default timer from global config or use the request provided value as seconds
	var timeout time.Duration
	if req.Timeout == nil {
		timeout = s.config.DefaultTransactionTimeout
	} else {
		timeout = time.Duration(*req.Timeout) * time.Second
	}

	// retrieve the referenced datastore
	ds, err := s.getDataStore(req.DatastoreName)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	// create a list of intents that are contained in the req.
	transactionIntents := make([]*types.TransactionIntent, 0, len(req.GetIntents()))

	// populate the transactionIntents from the req.intents basically transforming from sdcpb to datastore intent format
	for _, intent := range req.GetIntents() {
		ti, err := ds.SdcpbTransactionIntentToInternalTI(ctx, intent)
		if err != nil {
			return nil, err
		}

		transactionIntents = append(transactionIntents, ti)
	}

	// perform the conversion also for the replace intent
	var replaceIntent *types.TransactionIntent
	if req.GetReplaceIntent() != nil {
		// overwrite replace priority and name with specific value
		req.ReplaceIntent.Priority = tree.ReplaceValuesPrio
		req.ReplaceIntent.Intent = tree.ReplaceIntentName

		replaceIntent, err = ds.SdcpbTransactionIntentToInternalTI(ctx, req.GetReplaceIntent())
		if err != nil {
			return nil, err
		}
	}

	// with the transformed intents call the TransactionSet() on the datastore.
	rsp, err := ds.TransactionSet(ctx, req.GetTransactionId(), transactionIntents, replaceIntent, timeout, req.GetDryRun())
	if errors.Is(err, datastore.ErrDatastoreLocked) {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}

	return rsp, err
}

func (s *Server) TransactionConfirm(ctx context.Context, req *sdcpb.TransactionConfirmRequest) (*sdcpb.TransactionConfirmResponse, error) {
	pr, _ := peer.FromContext(ctx)
	log.Debugf("received TransactionConfirm request %v from peer %s", req, pr.Addr.String())

	if req.GetDatastoreName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name")
	}

	ds, err := s.getDataStore(req.DatastoreName)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	err = ds.TransactionConfirm(ctx, req.TransactionId)
	if errors.Is(err, datastore.ErrDatastoreLocked) {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	return nil, err
}

func (s *Server) TransactionCancel(ctx context.Context, req *sdcpb.TransactionCancelRequest) (*sdcpb.TransactionCancelResponse, error) {
	pr, _ := peer.FromContext(ctx)
	log.Debugf("received TransactionCancel request %v from peer %s", req, pr.Addr.String())

	if req.GetDatastoreName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing datastore name")
	}

	ds, err := s.getDataStore(req.DatastoreName)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	err = ds.TransactionCancel(ctx, req.TransactionId)
	if errors.Is(err, datastore.ErrDatastoreLocked) {
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	return nil, err
}
