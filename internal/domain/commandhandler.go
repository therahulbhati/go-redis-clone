package domain

import "net"

type CommandHandler interface {
	HandleClient(conn net.Conn)
	ProcessCommand(parts []string, conn net.Conn)
}
