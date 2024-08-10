package handler

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/therahulbhati/go-redis-clone/internal/domain"
	"github.com/therahulbhati/go-redis-clone/pkg/resp"
)

type CommandHandler struct {
	store     domain.Store
	leaderMgr domain.LeaderManager
	prevWrite bool
}

func debugLog(format string, v ...interface{}) {
	fmt.Printf("[DEBUG] "+format+"\n", v...)
}

// NewCommandHandler creates a new command handler.
func NewCommandHandler(store domain.Store, leaderMgr domain.LeaderManager) domain.CommandHandler {
	return &CommandHandler{
		store:     store,
		leaderMgr: leaderMgr,
		prevWrite: false,
	}
}

func (ch *CommandHandler) HandleClient(conn net.Conn) {
	//defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		request, err := resp.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				fmt.Printf("Connection closed by client: %s\n", conn.RemoteAddr())
			} else {
				fmt.Printf("Error reading from %s: %s\n", conn.RemoteAddr(), err.Error())
			}
			return
		}

		ch.ProcessCommand(request, conn)
	}
}

func (ch *CommandHandler) ProcessCommand(parts []string, conn net.Conn) {
	if len(parts) == 0 {
		conn.Write([]byte(resp.EncodeRESPError("empty command provided")))
		return
	}
	switch strings.ToUpper(parts[0]) {
	case "PING":
		conn.Write([]byte(resp.EncodeRESPSimpleString("PONG")))
	case "ECHO":
		if len(parts) < 2 {
			conn.Write([]byte(resp.EncodeRESPError("wrong number of arguments for 'echo' command")))
			return
		}
		conn.Write([]byte(resp.EncodeRESPString(parts[1])))
	case "SET":
		if len(parts) < 3 {
			conn.Write([]byte(resp.EncodeRESPError("wrong number of arguments for 'set' command")))
			return
		}
		key, value := parts[1], parts[2]
		expiration := time.Duration(-1)
		fmt.Println("parts", parts)
		if len(parts) == 5 && strings.ToUpper(parts[3]) == "PX" {
			px, err := strconv.Atoi(parts[4])
			if err != nil {
				conn.Write([]byte(resp.EncodeRESPError("invalid PX value")))
				return
			}

			expiration = time.Duration(px) * time.Millisecond
		}
		debugLog("SET command received, key: %s, value: %s, expiration: %d", key, value, expiration)
		ch.store.Set(key, value, expiration)
		conn.Write([]byte(resp.EncodeRESPSimpleString("OK")))
		if ch.leaderMgr != nil {
			ch.leaderMgr.PropagateCommand(parts)
		}
		ch.prevWrite = true
		debugLog("SET command executed, prevWrite set to true")
	case "GET":
		if len(parts) != 2 {
			conn.Write([]byte(resp.EncodeRESPError("wrong number of arguments for 'get' command")))
			return
		}
		value, exists := ch.store.Get(parts[1])
		if !exists {
			conn.Write([]byte(resp.EncodeRESPNull()))
			return
		}
		conn.Write([]byte(resp.EncodeRESPString(value)))
	case "INFO":
		conn.Write([]byte(ch.handleInfo(parts)))
	case "REPLCONF":
		if parts[1] == "ACK" {
			return
		}
		conn.Write([]byte(resp.EncodeRESPSimpleString("OK")))
	case "PSYNC":
		ch.handlePSync(parts, conn)
	case "WAIT":
		ch.handleWait(parts, conn)
	default:
		conn.Write([]byte(resp.EncodeRESPError("unknown command '" + parts[0] + "'")))
	}
}

func (ch *CommandHandler) handlePSync(parts []string, conn net.Conn) {
	if ch.leaderMgr == nil {
		conn.Write([]byte(resp.EncodeRESPError("PSYNC only supported by leader")))
		return
	}
	if err := ch.leaderMgr.SendFullResync(conn); err != nil {
		conn.Write([]byte(resp.EncodeRESPError(err.Error())))
		return
	}
	ch.leaderMgr.AddFollower(conn)

}

func (ch *CommandHandler) handleInfo(parts []string) string {
	var info strings.Builder
	if ch.leaderMgr != nil {
		info.WriteString("role:leader\n")
		info.WriteString(fmt.Sprintf("leader_replid:%s\n", ch.leaderMgr.GetLeaderReplID()))
		info.WriteString(fmt.Sprintf("leader_repl_offset:%d\n", ch.leaderMgr.GetLeaderReplOffset()))
	} else {
		info.WriteString("role:follower\n")
	}
	return resp.EncodeRESPString(info.String())
}

func (ch *CommandHandler) handleWait(parts []string, conn net.Conn) {
	if ch.leaderMgr == nil {
		conn.Write([]byte(resp.EncodeRESPError("WAIT only supported by leader")))
		return
	}

	if !ch.prevWrite {
		conn.Write([]byte(resp.EncodeRESPInteger(int64(ch.leaderMgr.GetFollowerCount()))))
		return
	}

	if len(parts) < 3 {
		conn.Write([]byte(resp.EncodeRESPError("wrong number of arguments for 'wait' command")))
		return
	}

	numReplicas, err := strconv.Atoi(parts[1])
	if err != nil {
		conn.Write([]byte(resp.EncodeRESPError("invalid number of replicas")))
		return
	}

	timeout, err := strconv.Atoi(parts[2])
	if err != nil {
		conn.Write([]byte(resp.EncodeRESPError("invalid timeout")))
		return
	}

	acks, err := ch.leaderMgr.WaitForAcknowledgments(numReplicas, timeout)
	if err != nil {
		conn.Write([]byte(resp.EncodeRESPError(err.Error())))
		return
	}

	conn.Write([]byte(resp.EncodeRESPInteger(int64(acks))))
	ch.prevWrite = false // Reset prevWrite after WAIT
	debugLog("WAIT command completed, prevWrite reset to false")
}
