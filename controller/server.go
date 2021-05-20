package controller

import (
	"context"
	"github.com/Nystya/distributed-commit/domain"
	pb "github.com/Nystya/distributed-commit/grpc/proto-files/service"
	pbservice "github.com/Nystya/distributed-commit/grpc/proto-files/service"
	"github.com/Nystya/distributed-commit/service"
	"github.com/golang/protobuf/ptypes/empty"
)

type CommitServer struct {
	pb.UnimplementedCommitServer

	participant service.Participant
}

func NewCommitServer(participant service.Participant) *CommitServer {
	return &CommitServer{
		UnimplementedCommitServer: pbservice.UnimplementedCommitServer{},
		participant:               participant,
	}
}

func (c *CommitServer) Prepare(ctx context.Context, request *pbservice.TransactionRequest) (*pbservice.TransactionResponse, error) {
	decision := pb.Decision_COMMIT

	txID := request.GetTransactionID().GetID()
	key := request.GetKey()
	value := request.GetValue()

	if err := c.participant.HandlePrepare(txID, key, value); err != nil {
		decision = pb.Decision_ABORT
	}

	return &pb.TransactionResponse{
		ID:       request.TransactionID,
		Decision: decision,
	}, nil
}

func (c *CommitServer) Commit(ctx context.Context, id *pbservice.TransactionID) (*pbservice.TransactionResponse, error) {

	txID := id.GetID()

	go func() {
		_ = c.participant.HandleCommit(txID)
	}()

	return &pb.TransactionResponse{
		ID:       id,
		Decision: pb.Decision_COMMIT,
	}, nil
}

func (c *CommitServer) Abort(ctx context.Context, id *pbservice.TransactionID) (*pbservice.TransactionResponse, error) {

	txID := id.GetID()

	go func() {
		_ = c.participant.HandleAbort(txID)
	}()

	return &pb.TransactionResponse{
		ID:       id,
		Decision: pb.Decision_ABORT,
	}, nil
}

func (c *CommitServer) Get(ctx context.Context, key *pbservice.Key) (*pbservice.DataResponse, error) {

	value, err := c.participant.Get(key.GetKey())
	if err != nil {
		return nil, err
	}

	return &pb.DataResponse{
		Value: value,
	}, nil
}

func (c *CommitServer) GetStatus(ctx context.Context, txID *pbservice.TransactionID) (*pbservice.TransactionResponse, error) {
	status, err := c.participant.GetStatus(txID.GetID())
	if err != nil {
		return nil, err
	}

	var decision pb.Decision

	switch status {
	case domain.Ready:
		decision = pb.Decision_READY
	case domain.Commit:
		decision = pb.Decision_COMMIT
	case domain.Abort:
		decision = pb.Decision_ABORT
	}

	return &pb.TransactionResponse{
		ID:       txID,
		Decision: decision,
	}, nil
}

func (c *CommitServer) Fail(ctx context.Context, request *pbservice.FailRequest) (*empty.Empty, error) {
	return nil, nil
}
