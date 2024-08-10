package main

import (
	"flag"
	"fmt"
	"github.com/therahulbhati/go-redis-clone/internal/domain"
	"net"
	"os"
	"strings"

	"github.com/therahulbhati/go-redis-clone/internal/handler"
	"github.com/therahulbhati/go-redis-clone/internal/replication"
	"github.com/therahulbhati/go-redis-clone/internal/storage"
)

func main() {
	port := flag.String("port", "6379", "Port to run the Redis server on")
	replicaof := flag.String("replicaof", "", "Replicate another Redis server")

	flag.Parse()

	store := storage.NewInMemoryStore()

	if *replicaof == "" {
		fmt.Println("Starting as Leader")
		leaderMgr := replication.NewLeader()
		commandHandler := handler.NewCommandHandler(store, leaderMgr)
		startServer(*port, commandHandler)
	} else {
		fmt.Println("Starting as Follower")

		leaderInfo := strings.Split(*replicaof, " ")
		leaderHost, leaderPort := leaderInfo[0], leaderInfo[1]
		followerManager := replication.NewFollower(store, *port, leaderHost, leaderPort)

		if err := followerManager.ConnectToLeader(); err != nil {
			fmt.Printf("Failed to connect to leader: %v\n", err)
		}

		go followerManager.ReceiveAndProcessCommands()

		commandHandler := handler.NewCommandHandler(store, nil)
		startServer(*port, commandHandler)
	}
}

func startServer(port string, handler domain.CommandHandler) {
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("Server listening on port %s\n", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		go handler.HandleClient(conn)
	}
}
