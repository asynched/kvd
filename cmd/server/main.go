package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/asynched/kvd/config"
	"github.com/asynched/kvd/controllers"
	"github.com/asynched/kvd/managers"
	"github.com/gofiber/fiber/v2"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
)

var configFlag = flag.String("config", "config.json", "path to config file")

func main() {
	flag.Parse()

	config, err := config.ParseConfig(*configFlag)

	if err != nil {
		log.Fatal(err)
	}

	log.SetFlags(log.Ldate | log.Ltime | log.Lmsgprefix)
	log.SetPrefix(fmt.Sprintf("[%s] ", config.Name))

	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})

	log.Println("Initializing key-value manager")
	manager := managers.NewKeyValueManager()

	log.Println("Initializing RAFT node")
	raftNode, err := GetRaft(manager, config)

	if err != nil {
		log.Fatal(err)
	}

	manager.SetRaft(raftNode)

	if config.JoinAddr != "" {
		go JoinCluster(config, raftNode)
	}

	go func() {
		for {
			log.Printf("Leader is: %s\n", raftNode.Leader())
			time.Sleep(10 * time.Second)
		}
	}()

	log.Println("Setting up routes")
	dbController := controllers.NewDbController(manager)

	app.Post("/join", dbController.Join)
	app.Get("/db", dbController.GetAll)
	app.Get("/db/:key", dbController.GetOne)
	app.Post("/db", dbController.Create)
	app.Delete("/db/:key", dbController.Delete)

	serverAddress := fmt.Sprintf("%s:%d", config.Host, config.Port)
	log.Printf("Server is starting on address: http://%s\n", serverAddress)
	log.Fatal(app.Listen(serverAddress))
}

// RAFT
func GetRaft(fsm raft.FSM, appConfig config.Config) (*raft.Raft, error) {
	addr := fmt.Sprintf("%s:%d", appConfig.Host, appConfig.Port+1)

	transport, err := raft.NewTCPTransport(addr, nil, 3, 10*time.Second, nil)

	if err != nil {
		return nil, err
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(addr)

	logStore, err := boltdb.NewBoltStore(fmt.Sprintf("data/logs/%s.db", appConfig.Name))

	if err != nil {
		return nil, err
	}

	stableStore, err := boltdb.NewBoltStore(fmt.Sprintf("data/stable/%s.db", appConfig.Name))

	if err != nil {
		return nil, err
	}

	raftNode, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, raft.NewInmemSnapshotStore(), transport)

	if err != nil {
		return nil, err
	}

	if !appConfig.Bootstrap {
		return raftNode, nil
	}

	log.Println("Bootstrapping cluster")

	serverConfig := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raftConfig.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}

	if err := raftNode.BootstrapCluster(serverConfig).Error(); err != nil {
		log.Println("Cluster is already bootstrapped")
	}

	return raftNode, nil
}

func JoinCluster(config config.Config, raftNode *raft.Raft) {
	log.Println("Joining cluster")

	for retries := 0; retries <= 10; retries++ {
		f := raftNode.GetConfiguration()

		if f.Error() != nil {
			log.Println("Failed to get raft configuration")
		} else {
			if len(f.Configuration().Servers) > 0 {
				log.Println("Cluster already has nodes")
				return
			}
		}

		if err := JoinLeaderNode(config, raftNode.LastIndex()); err != nil {
			log.Println(err)
		} else {
			log.Println("Successfully joined cluster")

			log.Println("Raft state:", raftNode.GetConfiguration().Configuration())
			return
		}

		log.Printf("Failed to join cluster (retry %d/10)\n", retries+1)
		time.Sleep(5 * time.Second)
	}

	log.Fatal("Failed to join cluster (10 retries)")
}

func JoinLeaderNode(config config.Config, lastIndex uint64) error {
	var body struct {
		Address   string `json:"address"`
		LastIndex uint64 `json:"lastIndex"`
	}

	body.Address = fmt.Sprintf("%s:%d", config.Host, config.Port+1)
	body.LastIndex = lastIndex

	b, _ := json.Marshal(body)

	request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/join", config.JoinAddr),
		bytes.NewBuffer(b),
	)

	if err != nil {
		return err
	}

	request.Header.Set("Content-Type", "application/json")

	client := http.Client{}

	response, err := client.Do(request)

	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("unable to join cluster: %s, reason: %s", response.Status, string(body))
	}

	return nil
}
