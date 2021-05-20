package messaging

import (
	"context"
	"github.com/Nystya/distributed-commit/grpc/proto-files/service"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"log"
	"time"
)

type CommitClientConfig struct {
	PeerName	string
	ServerAddr 	string
}

type CommitClient struct {
	PeerName		string
	rpcCommitClient service.CommitClient
	serverAddr		string
}

func NewCommitClient(config *CommitClientConfig) *CommitClient {
	return &CommitClient{
		PeerName: config.PeerName,
		serverAddr: config.ServerAddr,
		rpcCommitClient: nil,
	}
}

func (c *CommitClient) Connect() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	rpcConn, err := grpc.DialContext(ctx, c.serverAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Client could not connect: %v\n", err)
		return err
	}

	log.Printf("Connected to: %v\n", c.serverAddr)

	c.rpcCommitClient = service.NewCommitClient(rpcConn)

	return nil
}

func (c *CommitClient) Prepare(ctx context.Context, req *service.TransactionRequest) (*service.TransactionResponse, error) {
	return c.rpcCommitClient.Prepare(ctx, req)
}

func (c *CommitClient) Commit(ctx context.Context, txID *service.TransactionID) (*service.TransactionResponse, error) {
	return c.rpcCommitClient.Commit(ctx, txID)
}

func (c *CommitClient) Abort(ctx context.Context, txID *service.TransactionID) (*service.TransactionResponse, error) {
	return c.rpcCommitClient.Abort(ctx, txID)
}

func (c *CommitClient) Get(ctx context.Context, key *service.Key) (*service.DataResponse, error) {
	return c.rpcCommitClient.Get(ctx, key)
}

func (c *CommitClient) GetStatus(ctx context.Context, txID *service.TransactionID) (*service.TransactionResponse, error) {
	return c.rpcCommitClient.GetStatus(ctx, txID)
}

func (c *CommitClient) Fail(ctx context.Context, req *service.FailRequest) (*empty.Empty, error) {
	return c.rpcCommitClient.Fail(ctx, req)
}
