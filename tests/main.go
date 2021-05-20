package main

import (
	"github.com/Nystya/distributed-commit/config"
	"github.com/Nystya/distributed-commit/service"
	"log"
	"time"
)

func main() {
	log.Println("Reading config")

	cfg := config.NewConfig()

	log.Println("Initializing coordinator service...")

	coordinatorService := service.NewTPCCoordinator(cfg.PeerList, true, cfg.Port)
	coordinatorService.Connect()

	time.Sleep(time.Second)
	time.Sleep(time.Second)
	time.Sleep(time.Second)
	time.Sleep(time.Second)

	log.Println("Starting to add data...")

	//put("ana", []byte("meritous"), coordinatorService)
	put("vali", []byte("tata"), coordinatorService)
	//put("cali", []byte("in"), coordinatorService)
	//put("cata", []byte("panere"), coordinatorService)

	time.Sleep(time.Second)

	get("ana", coordinatorService)
	get("vali", coordinatorService)
	get("cali", coordinatorService)
	get("cata", coordinatorService)
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

//func gather(key string, coordinator service.Coordinator) {
//	peersData, err := coordinator.Gather(key)
//	if err != nil {
//		return
//	}
//
//}