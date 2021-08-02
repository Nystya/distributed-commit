package main

import (
	"bufio"
	"github.com/Nystya/distributed-commit/config"
	"github.com/Nystya/distributed-commit/service"
	"log"
	"os"
)

func main() {
	log.Println("Reading config")

	cfg := config.NewConfig()

	log.Println("Initializing coordinator service...")

	coordinatorService := service.NewTPCCoordinator(cfg.PeerList, true, cfg.Port)
	coordinatorService.Connect()

	input := bufio.NewScanner(os.Stdin)

	log.Println("This test will:")
	log.Println("1. Write data for 4 keys")
	log.Println("2. Read data from all nodes")
	log.Println("3. Modify data for key 'ana'")
	log.Println("4. Read data for all keys")
	log.Println()

	log.Println("Transactions may fail! Each node has a 5% chance to timeout and 1% chance of totally crashing.")

	log.Println("Press enter to start the test")
	input.Scan()
	log.Println("Starting to add data...")

	put("ana", []byte("Ana are mere"), coordinatorService)
	put("vali", []byte("Vali are pere"), coordinatorService)
	put("mihai", []byte("Mihai are portocale"), coordinatorService)
	put("cata", []byte("Cata are gutui"), coordinatorService)

	log.Println("Press enter to read data")
	input.Scan()

	gather("ana", coordinatorService)
	gather("vali", coordinatorService)
	gather("mihai", coordinatorService)
	gather("cata", coordinatorService)

	log.Println("Press enter to modify data")
	input.Scan()

	put("ana", []byte("Ana are mere si ghiocei"), coordinatorService)

	log.Println("Press enter to read new data")
	input.Scan()

	gather("ana", coordinatorService)
	gather("vali", coordinatorService)
	gather("mihai", coordinatorService)
	gather("cata", coordinatorService)

	log.Println("Press enter to read data after recover")
	input.Scan()

	gather("ana", coordinatorService)
	gather("vali", coordinatorService)
	gather("mihai", coordinatorService)
	gather("cata", coordinatorService)
}

func put(key string, value []byte, coordinator service.Coordinator) {
	log.Printf("Trying to put {%v: %v}\n", key, value)

	err := coordinator.Put(key, value)
	if err != nil {
		log.Printf("Could not put: {%v: %v} :: %v\n", key, value, err.Error())
	}
}

func get(key string, coordinator service.Coordinator) {
	value, err := coordinator.Get(key)
	if err != nil {
		log.Printf("Could not get: {%v: } :: %v\n", "ana", err.Error())
	}

	log.Printf("Received %v\n", string(value));
}

func gather(key string, coordinator service.Coordinator) {
	peersData, err := coordinator.Gather(key)
	if err != nil {
		return
	}

	log.Println("Getting: ", key)
	for k, v := range peersData {
		log.Printf("%v = %v\n", k, string(v))
	}
	log.Println()
}