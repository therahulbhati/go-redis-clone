package domain

import "net"

// ReplicationManager defines the interface for managing replication.
type LeaderManager interface {
	SendFullResync(conn net.Conn) error
	GetLeaderReplID() string
	GetLeaderReplOffset() int64
	AddFollower(conn net.Conn)
	PropagateCommand(cmd []string)
	GetFollowerCount() int
	WaitForAcknowledgments(numReplicas int, timeout int) (int, error)
}

// FollowerManager defines the interface for follower-specific replication operations.
type FollowerManager interface {
	ConnectToLeader() error
	ReceiveAndProcessCommands()
}
