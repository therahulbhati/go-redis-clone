# Go Redis Clone

A lightweight, Redis-like in-memory data structure store implemented in Go. This project aims to provide basic Redis functionality with a focus on simplicity and educational purposes.

## Features

- In-memory key-value storage
- Support for basic Redis commands (SET, GET, PING, ECHO)
- Key expiration with millisecond precision
- Leader-Follower replication
- RESP (Redis Serialization Protocol) implementation

## Getting Started

### Prerequisites

- Go 1.15 or higher

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/go-redis-clone.git
   ```
2. Navigate to the project directory:
   ```
   cd go-redis-clone
   ```
3. Build the project:
   ```
   go build -o go-redis-clone cmd/server/main.go
   ```

### Usage

To start the server:

```
./go-redis-clone
```

By default, the server runs on port 6379. You can specify a different port using the `-port` flag:

```
./go-redis-clone -port 6380
```

To run as a follower of another Redis server:

```
./go-redis-clone -replicaof <leader-host> <leader-port>
```

## Supported Commands

- `PING`: Test the connection
- `ECHO`: Echo the given string
- `SET`: Set a key-value pair (with optional expiration)
- `GET`: Get the value of a key
- `INFO`: Get information about the server
- `REPLCONF`: Used in replication
- `PSYNC`: Used in replication
- `WAIT`: Wait for replication

## Architecture

The project is structured into several packages:

- `main`: Entry point of the application
- `handler`: Handles incoming commands
- `storage`: Implements the in-memory store
- `replication`: Manages leader-follower replication
- `domain`: Defines interfaces and common types
- `resp`: Implements the RESP protocol

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the Redis project
- Created for educational purposes and to explore Go programming concepts

## Disclaimer

This is an educational project and is not intended for production use. For a robust, feature-complete Redis implementation, please use the official Redis project.
