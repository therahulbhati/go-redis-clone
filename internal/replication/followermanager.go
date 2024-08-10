package replication

import (
	"bufio"
	"fmt"
	"github.com/therahulbhati/go-redis-clone/internal/domain"
	"github.com/therahulbhati/go-redis-clone/pkg/resp"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Follower struct {
	conn           net.Conn
	port           string
	store          domain.Store
	reader         *bufio.Reader
	replID         string
	replOffset     int64
	leaderHost     string
	leaderPort     string
	mu             sync.Mutex
	commandHandler domain.CommandHandler
}

// NewFollower creates a new follower manager.
func NewFollower(store domain.Store, port, leaderHost, leaderPort string, commandHandler domain.CommandHandler) domain.FollowerManager {
	return &Follower{
		port:           port,
		store:          store,
		leaderHost:     leaderHost,
		leaderPort:     leaderPort,
		commandHandler: commandHandler,
	}
}

func (f *Follower) ConnectToLeader() error {
	var err error
	f.conn, err = net.Dial("tcp", f.leaderHost+":"+f.leaderPort)
	if err != nil {
		return fmt.Errorf("failed to connect to leader: %w", err)
	}
	f.reader = bufio.NewReader(f.conn)

	// Initiate the replication handshake
	if err := f.initiateReplication(); err != nil {
		return fmt.Errorf("failed to initiate replication: %w", err)
	}

	return nil
}

func (f *Follower) initiateReplication() error {
	if f.conn == nil {
		return fmt.Errorf("connection is nil in initiateReplication")
	}

	// Send PING command
	pingCmd := resp.EncodeRESPArray([]string{"PING"})
	if _, err := f.conn.Write([]byte(pingCmd)); err != nil {
		return fmt.Errorf("failed to send PING: %w", err)
	}
	response, err := f.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read PING response: %w", err)
	}
	debugLog("Received response from leader: %s", response)

	// Send REPLCONF listening-port
	replconfCmd := resp.EncodeRESPArray([]string{"REPLCONF", "listening-port", f.port})
	if _, err := f.conn.Write([]byte(replconfCmd)); err != nil {
		return fmt.Errorf("failed to send REPLCONF listening-port: %w", err)
	}
	response, err = f.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read REPLCONF listening-port response: %w", err)
	}
	debugLog("Received response from leader: %s", response)

	// Send REPLCONF capa psync2
	replconfCapaCmd := resp.EncodeRESPArray([]string{"REPLCONF", "capa", "psync2"})
	if _, err := f.conn.Write([]byte(replconfCapaCmd)); err != nil {
		return fmt.Errorf("failed to send REPLCONF capa: %w", err)
	}
	response, err = f.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read REPLCONF capa response: %w", err)
	}
	debugLog("Received response from leader: %s", response)

	// Send PSYNC command
	psyncCmd := resp.EncodeRESPArray([]string{"PSYNC", "?", "-1"})
	if _, err := f.conn.Write([]byte(psyncCmd)); err != nil {
		return fmt.Errorf("failed to send PSYNC: %w", err)
	}

	// Read FULLRESYNC response
	response, err = f.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read FULLRESYNC response: %w", err)
	}
	debugLog("Received response from leader: %s", response)

	parts := strings.Fields(strings.TrimSpace(response))
	if len(parts) != 3 || parts[0] != "+FULLRESYNC" {
		return fmt.Errorf("unexpected PSYNC response: %s", response)
	}

	f.replID = parts[1]
	f.replOffset, err = strconv.ParseInt(parts[2], 10, 64)
	debugLog("Reploffset: %d", f.replOffset)
	if err != nil {
		return fmt.Errorf("invalid replication offset: %w", err)
	}

	// Read RDB file size
	rdbSizeStr, err := f.reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read RDB size: %w", err)
	}
	if !strings.HasPrefix(rdbSizeStr, "$") {
		return fmt.Errorf("invalid RDB size format: %s", rdbSizeStr)
	}
	rdbSize, err := strconv.Atoi(strings.TrimSpace(rdbSizeStr[1:]))
	if err != nil {
		return fmt.Errorf("invalid RDB size: %w", err)
	}

	// Read RDB file content
	rdbContent := make([]byte, rdbSize)
	_, err = io.ReadFull(f.reader, rdbContent)
	if err != nil {
		return fmt.Errorf("failed to read RDB content: %w", err)
	}

	log.Printf("Successfully read RDB file of %d bytes", rdbSize)

	return nil
}

func (f *Follower) ReceiveAndProcessCommands() {
	debugLog("ReceiveAndProcessCommands started")
	for {
		command, err := resp.ParseRESP(f.reader)
		if err != nil {
			if err == io.EOF {
				log.Println("Connection to leader closed. Attempting to reconnect...")
				f.reconnectToLeader()
				continue
			}
			log.Printf("Error parsing RESP: %v", err)
			continue
		}
		debugLog("ReceiveAndProcessCommands: Follower processing command: %v", command)
		f.ProcessReplicationCommand(command)
		debugLog("ReceiveAndProcessCommands: Follower processed command: %v", command)
	}
}

func (f *Follower) reconnectToLeader() {
	for {
		err := f.ConnectToLeader()
		if err == nil {
			log.Println("Reconnected to leader successfully")
			return
		}
		log.Printf("Failed to reconnect to leader: %v. Retrying in 5 seconds...", err)
		time.Sleep(5 * time.Second)
	}
}

func (f *Follower) ProcessReplicationCommand(parts []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	debugLog("Follower processing command: %v", parts)

	switch strings.ToUpper(parts[0]) {

	case "REPLCONF":
		if len(parts) == 3 && strings.ToUpper(parts[1]) == "GETACK" {
			debugLog("Follower received REPLCONF GETACK, current offset: %d", f.replOffset)
			f.sendAck()
		}
	default:
		f.commandHandler.ProcessCommand(parts, f.conn)
	}
	f.replOffset += int64(len(resp.EncodeRESPArray(parts)))
}

func (f *Follower) sendAck() {
	ackResponse := resp.EncodeRESPArray([]string{"REPLCONF", "ACK", fmt.Sprint(f.replOffset)})
	_, err := f.conn.Write([]byte(ackResponse))
	if err != nil {
		debugLog("Error sending ACK: %v", err)
	} else {
		debugLog("ACK sent successfully with offset %d", f.replOffset)
	}
}
