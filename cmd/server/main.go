package main

import (
	"flag"
	"fmt"
	"github.com/therahulbhati/go-redis-clone/internal/domain"
	"github.com/therahulbhati/go-redis-clone/internal/rdb"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/therahulbhati/go-redis-clone/internal/handler"
	"github.com/therahulbhati/go-redis-clone/internal/replication"
	"github.com/therahulbhati/go-redis-clone/internal/storage"
)

func main() {
	port := flag.String("port", "6379", "Port to run the Redis server on")
	replicaof := flag.String("replicaof", "", "Replicate another Redis server")
	rdbFileDir := flag.String("dir", "", "Directory to store RDB file")
	rdbFileName := flag.String("dbfilename", "", "Name of the RDB file")

	flag.Parse()

	store := storage.NewInMemoryStore()
	// Load RDB file if it exists
	if *rdbFileDir != "" && *rdbFileName != "" {
		rdbFilePath := filepath.Join(*rdbFileDir, *rdbFileName)
		if _, err := os.Stat(rdbFilePath); err == nil {
			err := rdb.LoadRDBFile(rdbFilePath, store)
			if err != nil {
				fmt.Printf("Error loading RDB file: %v", err)
			} else {
				fmt.Printf("Successfully loaded RDB file: %s", rdbFilePath)
			}
		} else {
			fmt.Printf("RDB file does not exist at path: %s", rdbFilePath)
		}
	}

	if *replicaof == "" {
		fmt.Println("Starting as Leader")
		leaderMgr := replication.NewLeader()
		commandHandler := handler.NewCommandHandler(store, leaderMgr)
		startServer(*port, commandHandler)
	} else {
		fmt.Println("Starting as Follower")

		leaderInfo := strings.Split(*replicaof, " ")
		leaderHost, leaderPort := leaderInfo[0], leaderInfo[1]

		commandHandler := handler.NewCommandHandler(store, nil)
		followerManager := replication.NewFollower(store, *port, leaderHost, leaderPort, commandHandler)

		if err := followerManager.ConnectToLeader(); err != nil {
			fmt.Printf("Failed to connect to leader: %v\n", err)
		}

		go followerManager.ReceiveAndProcessCommands()

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
