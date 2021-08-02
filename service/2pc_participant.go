package service

import (
	"github.com/Nystya/distributed-commit/domain"
	"github.com/Nystya/distributed-commit/repository/database"
	"log"
	"math/rand"
	"sync"
	"time"
)

type TPCParticipant struct {
	log       			database.Database
	dataCache 			database.Database
	txCache   			database.Database

	lock				*sync.Mutex
	lockMap				map[string]*sync.Mutex

	recoverLock			*sync.Mutex
	recoverWg			*sync.WaitGroup
	commitWg			*sync.WaitGroup

	committingLockMap 	map[string]*sync.Mutex

	coordinatorService 	Coordinator
}

func NewTPCParticipant(log database.Database, dataCache database.Database, txCache database.Database, coordinatorService *TPCCoordinator) *TPCParticipant {
	return &TPCParticipant{
		log:    log,
		dataCache: dataCache,
		txCache: txCache,
		lock: &sync.Mutex{},
		lockMap: make(map[string]*sync.Mutex),
		recoverLock: &sync.Mutex{},
		recoverWg: &sync.WaitGroup{},
		commitWg: &sync.WaitGroup{},
		committingLockMap: make(map[string]*sync.Mutex),
		coordinatorService: coordinatorService,
	}
}

func (t *TPCParticipant) HandlePrepare(txID string, key string, value []byte) error {
	mayFail(6 * time.Second)

	// Make sure a recovery is not running right now
	t.recoverLock.Lock()
	t.recoverWg.Wait()
	t.recoverLock.Unlock()

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

	// Lock this resource (key) until it is committed or aborted
	lock.Lock()

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
	_ = t.dataCache.Put(key, entry)

	log.Println("Prepared: ", entry)

	// Start a timeout for the global decision
	// Used if the global decision never makes it to this node
	go t.awaitGlobalDecision(txID)
	
	return nil
}

func (t *TPCParticipant) awaitGlobalDecision(txID string) {
	time.Sleep(10 * time.Second)

	tx, err2 := t.txCache.Get(txID)
	if err2 != nil {
		panic("Could not recover from a failure, better shut down")
	}

	// If still in ready state, then global decision has not arrived
	if tx.State == domain.Ready {
		retries := 0
		maxRetries := 5

		err3 := t.Recover(true)
		for err3 != nil {
			if retries > maxRetries {
				panic("Could not recover from a failure, better shut down")
			}

			err3 = t.Recover(true)

			retries++
		}
	}
}

func (t *TPCParticipant) HandleCommit(txID string) error {
	mayCrash()
	mayFail(11 * time.Second)

	// Make sure a recovery is not running right now
	t.recoverLock.Lock()
	t.recoverWg.Wait()

	t.commitWg.Add(1)
	defer t.commitWg.Done()

	t.recoverLock.Unlock()

	t.lock.Lock()
	entry, err := t.txCache.Get(txID)
	if err != nil {
		return err
	}

	lock := t.lockMap[entry.Key]

	t.lock.Unlock()

	// Check if already committed by recovery process
	if entry.State != domain.Ready {
		return nil
	}

	// Release this resource (key) when finished committing
	defer lock.Unlock()

	entry.State = domain.Commit
	err = t.log.Put(entry.Key, entry)
	if err != nil {
		return err
	}

	_ = t.dataCache.Put(entry.Key, entry)
	_ = t.dataCache.Commit(entry.Key)
	_ = t.txCache.Put(txID, entry)

	log.Println("Committed: ", entry)

	return nil
}

func (t *TPCParticipant) handleCommit(txID string, shouldUnlock bool) error {
	t.lock.Lock()
	entry, err := t.txCache.Get(txID)
	if err != nil {
		return err
	}

	lock := t.lockMap[entry.Key]

	t.lock.Unlock()

	// Check if already committed by recovery process
	if entry.State != domain.Ready {
		return nil
	}

	// Release this resource (key) when finished committing
	if shouldUnlock {
		defer lock.Unlock()
	}

	entry.State = domain.Commit

	err = t.log.Put(entry.Key, entry)
	if err != nil {
		return err
	}

	_ = t.dataCache.Put(entry.Key, entry)
	_ = t.dataCache.Commit(entry.Key)
	_ = t.txCache.Put(txID, entry)

	log.Println("Committed: ", entry)

	return nil
}

func (t *TPCParticipant) HandleAbort(txID string) error {
	mayCrash()
	mayFail(11 * time.Second)

	// Make sure a recovery is not running right now
	t.recoverLock.Lock()
	t.recoverWg.Wait()

	t.commitWg.Add(1)
	defer t.commitWg.Done()

	t.recoverLock.Unlock()

	t.lock.Lock()
	entry, err := t.txCache.Get(txID)
	if err != nil {
		return err
	}

	if entry == nil {
		return nil
	}

	lock := t.lockMap[entry.Key]
	t.lock.Unlock()

	// Check if already aborted by recovery process
	if entry.State != domain.Ready {
		return nil
	}

	// Release this resource (key) when finished aborting
	defer lock.Unlock()

	entry.State = domain.Abort

	err = t.log.Put(entry.Key, entry)
	if err != nil {
		return err
	}

	_ = t.dataCache.Rollback(entry.Key)

	log.Println("Aborted: ", entry)

	return nil
}

func (t *TPCParticipant) handleAbort(txID string, shouldUnlock bool) error {
	t.lock.Lock()
	entry, err := t.txCache.Get(txID)
	if err != nil {
		return err
	}

	if entry == nil {
		return nil
	}

	lock := t.lockMap[entry.Key]
	t.lock.Unlock()

	// Check if already aborted by recovery process
	if entry.State != domain.Ready {
		return nil
	}

	// Release this resource (key) when finished aborting
	if shouldUnlock {
		defer lock.Unlock()
	}

	entry.State = domain.Abort

	err = t.log.Put(entry.Key, entry)
	if err != nil {
		return err
	}

	_ = t.dataCache.Rollback(entry.Key)

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

	log.Println("Waiting to get lock...")
	lock.Lock()
	defer lock.Unlock()

	log.Println("Got lock...")

	entry, err := t.dataCache.Get(key)
	if err != nil {
		return nil, err
	}

	// In case there's a transaction going on with this key,
	// wait for the transaction to conclude. Ugly busy-wait.
	for entry.State == domain.Ready {
		entry, err = t.dataCache.Get(key)
		if err != nil {
			return nil, err
		}
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

func (t *TPCParticipant) Recover(shouldUnlock bool) error {
	// Make sure a recovery is not running right now
	t.recoverLock.Lock()

	// We should not run two recoveries in parallel
	t.recoverWg.Wait()

	// Prevent other commits from starting
	t.recoverWg.Add(1)
	defer t.recoverWg.Done()

	// Await all running commits to start
	t.commitWg.Wait()

	t.recoverLock.Unlock()

	entryList, err := t.log.Recover()
	if err != nil {
		return err
	}

	// Used to enable parallel transaction processing
	// as long as we don't process transactions on the same key
	txLockMap := make(map[string]*sync.Mutex)

	// Used to keep the latest records about a key
	// so we only replay these records, not all entries
	commitMap := make(map[string]*domain.Entry)

	// First replay last commits
	for _, entry := range entryList {
		err := t.txCache.Put(entry.TxID, entry)
		if err != nil {
			return err
		}

		if _, ok := txLockMap[entry.Key]; !ok {
			txLockMap[entry.Key] = &sync.Mutex{}
		}

		if entry.State == domain.Commit {
			commitMap[entry.Key] = entry
		}
	}

	// First replay last commits
	wg := &sync.WaitGroup{}
	for _, tx := range commitMap {
		wg.Add(1)
		go func(tx *domain.Entry) {
			defer wg.Done()

			txLockMap[tx.Key].Lock()
			defer txLockMap[tx.Key].Unlock()

			// If tx was committed, save the data
			log.Println("Committing transaction: ", tx)

			// Check if key exists, otherwise create it
			if _, ok := t.lockMap[tx.Key]; !ok {
				t.lockMap[tx.Key] = &sync.Mutex{}
			}

			err := t.dataCache.Put(tx.Key, tx)
			if err != nil {
				panic("Could not save recovery data, better stop...")
			}
			_ = t.dataCache.Commit(tx.Key)

			committedTx, err := t.dataCache.Get(tx.Key)

			log.Println("Committed transaction: ", committedTx)
		}(tx)
	}

	wg.Wait()

	// Secondly check if there are any pending (Ready) transactions
	type Pair struct {
		pending bool
		data 	*domain.Entry
	}

	pendingStack := make(map[string]*Pair)
	for _, entry := range entryList {
		pair := &Pair{
			data:    entry,
		}

		if entry.State == domain.Ready {
			pair.pending = true
			pendingStack[entry.TxID] = pair
		} else {
			pair.pending = false
			pendingStack[entry.TxID] = pair
		}
	}

	pendingMap := make(map[string]*domain.Entry)
	for _, pair := range pendingStack {
		if pair.pending {
			pendingMap[pair.data.Key] = pair.data
		}
	}

	// Replay all pending transactions
	for _, tx := range pendingMap {
		wg.Add(1)

		go func(tx *domain.Entry) {
			defer wg.Done()
			txLockMap[tx.Key].Lock()
			defer txLockMap[tx.Key].Unlock()

			log.Printf("Recovering: %v = %v\n", tx, string(tx.Data))

			// Replay only pending transactions. Committed transactions were already played.
			if tx.State == domain.Ready {
				_ = t.dataCache.Put(tx.Key, tx)

				// If tx was still in "ready" ask other
				// nodes for actual state
				resolved := false

				for !resolved {
					status, err := t.getPeerStatus(tx.TxID)
					if err != nil {
						continue
					}

					// Check peers' status
					switch status {
					case domain.Ready:
						// Keep retrying until someone responds with a global decision
						// This may never come to a resolution and may block forever
						resolved = false

						log.Println("Could not resolve yet, retrying in 2 seconds...")

						// Wait a bit before retrying
						time.Sleep(2 * time.Second)

					case domain.Commit:
						log.Println("Committing transaction: ", tx)

						// Check if key exists, otherwise create it
						if _, ok := t.lockMap[tx.Key]; !ok {
							t.lockMap[tx.Key] = &sync.Mutex{}
						}

						err := t.handleCommit(tx.TxID, shouldUnlock)
						if err != nil {
							continue
						}

						resolved = true
					case domain.Abort:
						log.Println("Aborting transaction: ", tx)

						err := t.handleAbort(tx.TxID, shouldUnlock)
						if err != nil {
							continue
						}

						resolved = true
					}
				}
			}
		}(tx)
	}

	wg.Wait()

	return nil
}

func (t *TPCParticipant) getPeerStatus(txID string) (domain.Status, error){
	return t.coordinatorService.GetStatus(txID)
}

func mayFail(duration time.Duration) {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	shouldFail := random.Float32()

	// A 5% chance to fail
	if shouldFail <= 0.03 {
		//time.Sleep(duration)
	}
}

func mayCrash() {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	shouldFail := random.Float32()

	// A 1% chance to fail
	if shouldFail <= 0.01 {
		//panic("Chaos Monkey killed me!")
	}
}