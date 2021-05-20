package service

import (
	"context"
	"github.com/Nystya/distributed-commit/domain"
	pb "github.com/Nystya/distributed-commit/grpc/proto-files/service"
	"github.com/Nystya/distributed-commit/repository/messaging"
	"github.com/google/uuid"
	"log"
	"strconv"
	"sync"
	"time"
)

const LOCALNAME = "LOCALHOST"
const LOCALHOST = "127.0.0.1:"

type TPCCoordinator struct {
	myself			*messaging.CommitClient
	rpcPeers		[]*messaging.CommitClient

	retryTimeout 	int
	maxRetries		int
}

func NewTPCCoordinator(peerList []string, includeMyself bool, myPort string) *TPCCoordinator {
	rpcPeers := make([]*messaging.CommitClient, 0)

	log.Println("Creating peers from list...")

	for i, peer := range peerList {

		peerConfig := &messaging.CommitClientConfig{
			PeerName: 	strconv.Itoa(i),
			ServerAddr: peer,
		}

		rpcPeers = append(rpcPeers, messaging.NewCommitClient(peerConfig))
	}

	log.Println("Finished creating peers: ", rpcPeers)

	log.Println("Creating self config...")

	var myself *messaging.CommitClient

	if includeMyself {
		mySelfConfig := &messaging.CommitClientConfig{
			PeerName:	LOCALNAME,
			ServerAddr: LOCALHOST + myPort,
		}

		log.Println("Getting myself as a peer...")
		myself = messaging.NewCommitClient(mySelfConfig)
	}

	log.Println("Finished getting myself as a peer...")

	return &TPCCoordinator{
		retryTimeout: 1,
		maxRetries: 3,
		myself: myself,
		rpcPeers: rpcPeers,
	}
}

func (t *TPCCoordinator) Connect() error {
	for _, rpcPeer := range append(t.rpcPeers, t.myself) {
		if  rpcPeer == nil {
			continue
		}

		err := rpcPeer.Connect()
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *TPCCoordinator) Put(key string, value []byte) error {
	newUUID := uuid.New()

	wg := sync.WaitGroup{}
	shouldAbort := false

	// Send prepare request to all peers, including myself
	for _, rpcPeer := range append(t.rpcPeers, t.myself) {
		wg.Add(1)

		go func(rpcPeer *messaging.CommitClient) {
			defer wg.Done()

			if rpcPeer == nil {
				return
			}

			request := &pb.TransactionRequest{
				Key:           key,
				Value:         value,
				TransactionID: &pb.TransactionID{ID: newUUID.String()},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
			defer cancel()

			prepareResponse, err := rpcPeer.Prepare(ctx, request)
			if err != nil {
				shouldAbort = true
				return
			}

			if *(prepareResponse.GetDecision().Enum()) == pb.Decision_ABORT {
				shouldAbort = true
			}
		}(rpcPeer)
	}

	// Wait for the response of all peers
	wg.Wait()

	// Make decision and broadcast it
	if !shouldAbort {
		t.commit(newUUID.String())
	} else {
		t.abort(newUUID.String())
	}

	return nil
}

func (t *TPCCoordinator) commit(txID string) {
	for _, rpcPeer := range append(t.rpcPeers, t.myself) {
		go func(rpcPeer *messaging.CommitClient) {
			if rpcPeer == nil {
				return
			}

			request := &pb.TransactionID{ID: txID}

			ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
			defer cancel()

			_, _ = rpcPeer.Commit(ctx, request)
		}(rpcPeer)
	}
}

func (t *TPCCoordinator) abort(txID string) {
	for _, rpcPeer := range append(t.rpcPeers, t.myself) {
		go func(rpcPeer *messaging.CommitClient) {
			if rpcPeer == nil {
				return
			}

			request := &pb.TransactionID{ID: txID}

			ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
			defer cancel()

			_, _ = rpcPeer.Abort(ctx, request)
		}(rpcPeer)
	}
}

func (t *TPCCoordinator) Get(key string) ([]byte, error) {
	// Get data from myself
	pbKey := &pb.Key{Key: key}

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()

	data, err := t.myself.Get(ctx, pbKey)
	if err != nil {
		return nil, err
	}

	return data.GetValue(), nil
}

func (t *TPCCoordinator) Gather(key string) (map[string][]byte, error) {
	// Get data from myself & all other peers

	pbKey := &pb.Key{Key: key}

	data := make(map[string][]byte)

	wg := &sync.WaitGroup{}

	for _, rpcPeer := range append(t.rpcPeers, t.myself) {
		wg.Add(1)

		go func(rpcPeer *messaging.CommitClient) {
			defer wg.Done()

			if rpcPeer == nil {
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
			defer cancel()

			resp, err := rpcPeer.Get(ctx, pbKey)
			if err != nil {
				data[rpcPeer.PeerName] = nil
				return
			}

			data[rpcPeer.PeerName] = resp.Value
		}(rpcPeer)
	}

	wg.Wait()

	return data, nil
}

func (t *TPCCoordinator) GetStatus(txID string) (domain.Status, error) {
	return t.GetStatusWithRetry(txID, 0)
}

func (t *TPCCoordinator) GetStatusWithRetry(txID string, retryCount int) (domain.Status, error) {
	tx := &pb.TransactionID{ID: txID}

	shouldCommit := false
	shouldAbort	 := false

	parentContext := context.Background()

	wg := &sync.WaitGroup{}

	for _, rpcPeer := range t.rpcPeers {
		wg.Add(1)

		go func(rpcPeer *messaging.CommitClient) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(parentContext, 3 * time.Second)
			defer cancel()

			resp, err := rpcPeer.GetStatus(ctx, tx)
			if err != nil || resp == nil {
				return
			}

			switch *resp.Decision.Enum() {
			case pb.Decision_READY:
			case pb.Decision_COMMIT:
				shouldCommit = true
				parentContext.Done()
				return
			case pb.Decision_ABORT:
				shouldAbort = true
				parentContext.Done()
				return
			}
		}(rpcPeer)
	}

	wg.Wait()

	// All of processes are still in ready
	if shouldCommit == shouldAbort == false {
		// Transaction is still pending, retry
		// May retry indefinitely

		if t.maxRetries < 0 || retryCount < t.maxRetries {
			time.Sleep(time.Second)

			return t.GetStatusWithRetry(txID, retryCount + 1)
		}
	}

	if shouldCommit {
		return domain.Commit, nil
	}

	if shouldAbort {
		return domain.Abort, nil
	}

	return domain.Ready, nil
}

