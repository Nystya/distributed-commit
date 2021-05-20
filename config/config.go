package config

import (
	"flag"
	"github.com/Nystya/distributed-commit/repository/database"
	"strings"
)

type Config struct {
	WalConfig 	*database.WriteAheadLogConfig
	PeerList	[]string
	Port 		string
}

func NewConfig() *Config {
	myPort 	 := flag.String("port", "5000", "my port")
	peers := flag.String("peers", "", "peers's addresses")
	flag.Parse()

	walConfig := &database.WriteAheadLogConfig{
		Dir:         "/home/guest/facultate/distributedAlgorithms/project/resources",
		MaxFileSize: 100,
		Prefix: *myPort,
	}

	peerList := strings.Split(*peers, ",")

	if peerList[0] == "" {
		peerList = make([]string, 0)
	}

	return &Config{
		WalConfig: walConfig,
		PeerList:  peerList,
		Port:      *myPort,
	}
}
