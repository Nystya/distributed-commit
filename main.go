package main

import (
	config "github.com/Nystya/distributed-commit/config"
	"github.com/Nystya/distributed-commit/controller"
	pbservice "github.com/Nystya/distributed-commit/grpc/proto-files/service"
	"github.com/Nystya/distributed-commit/repository/database"
	"github.com/Nystya/distributed-commit/service"
	"google.golang.org/grpc"
	"log"
	"net"
)

// TODO: Failure recover
// TODO: Test with multiple peers
// TODO: Implement fail - chaos monkey
// TODO: Optimizations PrA, PrC, 3PC

// TODO: Future work slide
// TODO: Filmulet
// TODO: Tracer
// TODO: 8 min

func main() {
	done := make(chan bool)

	log.Println("Reading config")
	cfg := config.NewConfig()

	log.Println("Initializing caches...")

	dataCache := database.NewMemoryDatabase()
	txCache := database.NewMemoryDatabase()

	log.Println("Initializing wal...")

	wal, err := database.NewFileDatabase(cfg.WalConfig)
	if err != nil {
		log.Fatalln("Could not create local write-ahead log: ", err.Error())
	}

	log.Println("Initializing coordinator service for recovery...")
	coordinatorService := service.NewTPCCoordinator(cfg.PeerList, false, cfg.Port)

	log.Println("Initializing server...")

	participantService := service.NewTPCParticipant(wal, dataCache, txCache, coordinatorService)

	log.Println("Recovering last state...")
	err = participantService.Recover()
	if err != nil {
		log.Fatalln("Could not recover state: ", err.Error())
	}

	commitServer := controller.NewCommitServer(participantService)

	log.Println("Getting listener on: ", cfg.Port)

	lis, err := net.Listen("tcp", "127.0.0.1:" + cfg.Port)
	if err != nil {
		log.Fatalln("Failed to start listening: ", err.Error())
	}

	log.Println("Starting server...")

	grpcServer := grpc.NewServer()
	pbservice.RegisterCommitServer(grpcServer, commitServer)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalln("Failed to start serving: ", err.Error())
		}
	}()

	err = coordinatorService.Connect()
	if err != nil {
		log.Fatalln("Failed to connect to peers: ", err.Error())
	}

	<- done
}
