package service

import (
	"github.com/Nystya/distributed-commit/domain"
	"github.com/Nystya/distributed-commit/repository/database"
	"log"
	"sync"
)

type TPCParticipant struct {
	log       			database.Database
	dataCache 			database.Database
	txCache   			database.Database

	lock				*sync.Mutex
	lockMap				map[string]*sync.Mutex

	coordinatorService 	Coordinator
}

func NewTPCParticipant(log database.Database, dataCache database.Database, txCache database.Database, coordinatorService *TPCCoordinator) *TPCParticipant {
	return &TPCParticipant{
		log:    log,
		dataCache: dataCache,
		txCache: txCache,
		lock: &sync.Mutex{},
		lockMap: make(map[string]*sync.Mutex),
		coordinatorService: coordinatorService,
	}
}

func (t *TPCParticipant) HandlePrepare(txID string, key string, value []byte) error {
	lock, ok := t.lockMap[key]
	if !ok {
		// It's ok to lock here, it's not almost never executed
		t.lock.Lock()

		// Check if another thread created this lock
		// in the meantime
		if lock, ok = t.lockMap[key]; !ok {
			t.lockMap[key] = &sync.Mutex{}
			lock = t.lockMap[key]
		}

		t.lock.Unlock()
	}

	// Lock the resource that is being updated
	lock.Lock()
	defer lock.Unlock()

	entry := &domain.Entry{
		TxID:  	txID,
		Key: 	key,
		State: 	domain.Ready,
		Len:   	len(value),
		Data:  	value,
	}

	err := t.log.Put(key, entry)
	if err != nil {
		log.Println("Could not write transaction to wal: ", err)
		return err
	}

	_ = t.txCache.Put(txID, entry)

	log.Println("Prepared: ", entry)

	return nil
}

func (t *TPCParticipant) HandleCommit(txID string) error {
	t.lock.Lock()
	entry, err := t.txCache.Get(txID)
	if err != nil {
		return err
	}

	lock := t.lockMap[entry.Key]

	t.lock.Unlock()

	lock.Lock()
	defer lock.Unlock()

	entry.State = domain.Commit

	err = t.log.Put(entry.Key, entry)
	if err != nil {
		return err
	}

	_ = t.txCache.Put(txID, entry)
	_ = t.dataCache.Put(entry.Key, entry)

	log.Println("Committed: ", entry)

	return nil
}

func (t *TPCParticipant) HandleAbort(txID string) error {
	entry, err := t.txCache.Get(txID)
	if err != nil {
		return err
	}

	if entry == nil {
		return nil
	}

	lock := t.lockMap[entry.Key]

	lock.Lock()
	defer lock.Unlock()

	entry.State = domain.Abort

	err = t.log.Put(entry.Key, entry)
	if err != nil {
		return err
	}

	log.Println("Aborted: ", entry)

	return nil
}

func (t *TPCParticipant) Get(key string) ([]byte, error) {
	t.lock.Lock()

	lock, ok := t.lockMap[key]
	if !ok {
		return nil, &domain.NotFoundError{}
	}

	t.lock.Unlock()

	lock.Lock()
	defer lock.Unlock()

	entry, err := t.dataCache.Get(key)
	if err != nil {
		return nil, err
	}

	return entry.Data, nil
}

func (t *TPCParticipant) GetStatus(txID string) (domain.Status, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	tx, err := t.txCache.Get(txID)
	if err != nil {
		return domain.Ready, err
	}

	return tx.State, nil
}

func (t *TPCParticipant) Recover() error {
	entryList, err := t.log.Recover()
	if err != nil {
		return err
	}

	log.Println("Entry List: ", entryList)

	for _, entry := range entryList {
		log.Println("Updating: ", entry)

		err := t.txCache.Put(entry.TxID, entry)
		if err != nil {
			return err
		}
	}

	for _, txID := range t.txCache.GetAllKeys() {
		tx, err := t.txCache.Get(txID)
		if err != nil {
			return err
		}

		log.Println("Recovering: ", tx)

		switch tx.State {
		case domain.Ready:
			// If tx was still in "ready" ask other
			// nodes for actual state
			status, err := t.getPeerStatus(tx.TxID)
			if err != nil {
				return err
			}

			// Check peers' status
			switch status {
			case domain.Ready:
				// Block I guess (coordinator may have died)
				// I cannot recover from this state.
				// Block forever

				log.Println("I cannot recover from this and I will block forever")

				wg := &sync.WaitGroup{}
				wg.Add(1)
				wg.Wait()
			case domain.Commit:
				log.Println("Committing transaction: ", tx)

				// Check if key exists, otherwise create it
				if _, ok := t.lockMap[tx.Key]; !ok {
					t.lockMap[tx.Key] = &sync.Mutex{}
				}

				err := t.dataCache.Put(tx.Key, tx)
				if err != nil {
					return err
				}
			case domain.Abort:
				log.Println("Aborting transaction: ", tx)
				continue
			}

		case domain.Commit:
			// If tx was committed, save the data
			log.Println("Committing transaction: ", tx)

			// Check if key exists, otherwise create it
			if _, ok := t.lockMap[tx.Key]; !ok {
				t.lockMap[tx.Key] = &sync.Mutex{}
			}

			err := t.dataCache.Put(tx.Key, tx)
			if err != nil {
				return err
			}

			committedTx, err := t.dataCache.Get(tx.Key)
			log.Println("Committed transaction: ", committedTx)
		case domain.Abort:
			// If tx was aborted, don't change the db
			log.Println("Aborting transaction: ", tx)
			continue
		}
	}

	return nil
}

func (t *TPCParticipant) getPeerStatus(txID string) (domain.Status, error){
	return t.coordinatorService.GetStatus(txID)
}
