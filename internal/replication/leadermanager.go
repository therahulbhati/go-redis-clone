package replication

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"github.com/therahulbhati/go-redis-clone/internal/domain"
	"github.com/therahulbhati/go-redis-clone/pkg/resp"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Leader struct {
	leaderReplID     string
	leaderReplOffset int64
	followers        []net.Conn
	mu               sync.Mutex
	ackChan          chan int64
	lastWrite        int64
}

func debugLog(format string, v ...interface{}) {
	fmt.Printf("[DEBUG] "+format+"\n", v...)
}

// NewLeader creates a new leader manager.
func NewLeader() domain.LeaderManager {
	return &Leader{
		leaderReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		leaderReplOffset: 0,
		followers:        make([]net.Conn, 0),
		ackChan:          make(chan int64, 100),
		lastWrite:        0,
	}
}

func (l *Leader) GetLeaderReplID() string {
	return l.leaderReplID
}

func (l *Leader) GetLeaderReplOffset() int64 {
	return l.leaderReplOffset
}

func (l *Leader) SendFullResync(conn net.Conn) error {
	if conn == nil {
		return fmt.Errorf("nil connection passed to SendFullResync")
	}

	// Send FULLRESYNC response
	fullResyncResponse := resp.EncodeRESPSimpleString(
		fmt.Sprintf("FULLRESYNC %s %d", l.leaderReplID, l.leaderReplOffset))
	_, err := conn.Write([]byte(fullResyncResponse))
	if err != nil {
		return fmt.Errorf("failed to send FULLRESYNC response: %w", err)
	}

	// Base64 representation of the empty RDB file
	rdbBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	// Decode the Base64 string to binary
	rdbData, err := base64.StdEncoding.DecodeString(rdbBase64)
	if err != nil {
		return fmt.Errorf("failed to decode base64 RDB data: %w", err)
	}

	data := strings.TrimRight(resp.EncodeRESPString(string(rdbData)), "\r\n")
	// Send the binary contents
	_, err = conn.Write([]byte(data))
	if err != nil {
		return fmt.Errorf("failed to send RDB contents: %w", err)
	}

	return nil
}

func (l *Leader) AddFollower(conn net.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.followers = append(l.followers, conn)
	go l.readAcks(conn)
	debugLog("Added new follower: %s", conn.RemoteAddr())
}

func (l *Leader) GetFollowerCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.followers)
}

func (l *Leader) PropagateCommand(cmd []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	debugLog("Propagating command: %v", cmd)
	encodedCmd := resp.EncodeRESPArray(cmd)
	for _, follower := range l.followers {
		_, err := follower.Write([]byte(encodedCmd))
		if err != nil {
			debugLog("Error propagating command to follower: %v", err)
		}
	}
	l.leaderReplOffset += int64(len(encodedCmd))
	l.lastWrite = l.leaderReplOffset
	debugLog("New leader replication offset: %d", l.leaderReplOffset)
}

func (l *Leader) sendReplconfGetack(conn net.Conn) error {
	debugLog("Sending REPLCONF GETACK to follower %s", conn.RemoteAddr())
	getackCmd := resp.EncodeRESPArray([]string{"REPLCONF", "GETACK", "*"})
	_, err := conn.Write([]byte(getackCmd))
	if err != nil {
		debugLog("Error sending REPLCONF GETACK: %v", err)
		return err
	}
	debugLog("REPLCONF GETACK sent successfully to %s", conn.RemoteAddr())
	return nil
}

func (l *Leader) WaitForAcknowledgments(numReplicas int, timeout int) (int, error) {
	debugLog("WaitForAcknowledgments called with numReplicas: %d, timeout: %d", numReplicas, timeout)
	debugLog("Current follower count: %d", len(l.followers))

	l.mu.Lock()
	currentOffset := l.lastWrite
	followers := l.followers
	l.mu.Unlock()

	for _, follower := range followers {
		if err := l.sendReplconfGetack(follower); err != nil {
			debugLog("Failed to send GETACK to follower %s: %v", follower.RemoteAddr(), err)
		}
	}

	acks := 0
	timeoutChan := time.After(time.Duration(timeout) * time.Millisecond)
	debugLog("Waiting for ACKs, current offset: %d", currentOffset)

	for {
		select {
		case offset := <-l.ackChan:
			debugLog("Received ACK with offset %d, current offset is %d", offset, currentOffset)
			if offset >= currentOffset {
				acks++
				debugLog("Valid ACK received. Total: %d/%d", acks, numReplicas)
				if acks >= numReplicas {
					debugLog("Received all required ACKs")
					return acks, nil
				}
			}
		case <-timeoutChan:
			debugLog("Timeout reached. Received %d/%d ACKs", acks, numReplicas)
			return acks, nil
		}
	}
}

func (l *Leader) readAcks(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
			debugLog("Error setting read deadline: %v", err)
			return
		}

		response, err := resp.ParseRESP(reader)
		debugLog("Received response from follower %s: %v", conn.RemoteAddr(), response)
		if err != nil {
			if err == io.EOF {
				debugLog("Connection closed for follower %s", conn.RemoteAddr())
				return
			}
			debugLog("Error reading from follower %s: %v", conn.RemoteAddr(), err)
			continue
		}

		debugLog("Received response from follower %s: %v", conn.RemoteAddr(), response)

		if len(response) == 3 && response[0] == "REPLCONF" && response[1] == "ACK" {
			offset, err := strconv.ParseInt(response[2], 10, 64)
			if err != nil {
				debugLog("Invalid ACK offset from follower %s: %v", conn.RemoteAddr(), err)
				continue
			}
			debugLog("Valid ACK received from follower %s with offset %d", conn.RemoteAddr(), offset)
			select {
			case l.ackChan <- offset:
				debugLog("ACK sent to channel for follower %s", conn.RemoteAddr())
			default:
				debugLog("ACK channel full, discarding ACK from follower %s", conn.RemoteAddr())
			}
		}
	}
}
