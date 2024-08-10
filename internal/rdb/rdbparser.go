package rdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/therahulbhati/go-redis-clone/internal/domain"
	"io"
	"os"
	"time"
)

func LoadRDBFile(filePath string, store domain.Store) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read and validate the header
	header := make([]byte, 9)
	if _, err := io.ReadFull(file, header); err != nil {
		return err
	}
	if !bytes.HasPrefix(header, []byte("REDIS")) {
		return errors.New("invalid RDB file format")
	}

	// Skip Metadata and parse database section
	for {
		var b byte
		if err := binary.Read(file, binary.BigEndian, &b); err != nil {
			return err
		}

		switch b {
		case 0xFE:
			_, err := readSize(file) // Read the database index (size encoded)
			if err != nil {
				return err
			}
			if err := parseDatabaseSection(file, store); err != nil {
				return err
			}
		case 0xFF: // End of file section
			return nil
		}
	}
}

func parseDatabaseSection(file *os.File, store domain.Store) error {
	var currentExpiration *time.Time

	for {
		var b byte
		if err := binary.Read(file, binary.BigEndian, &b); err != nil {
			return err
		}

		switch b {
		case 0xFB: // Resizedb field, indicates hash table size information
			// Skip the sizes of the hash table and the expire hash table
			if _, err := readSize(file); err != nil {
				return err
			}
			if _, err := readSize(file); err != nil {
				return err
			}

		case 0xFD: // Expiry time in seconds
			expiryTime, err := readUint32(file)
			if err != nil {
				return err
			}
			exp := time.Unix(int64(expiryTime), 0)
			currentExpiration = &exp

		case 0xFC: // Expiry time in milliseconds
			expiryTime, err := readUint64(file)
			if err != nil {
				return err
			}
			exp := time.Unix(0, int64(expiryTime)*int64(time.Millisecond))
			currentExpiration = &exp

		case 0x00: // Value type is string (assuming only string types for simplicity)
			key, err := readString(file)
			if err != nil {
				return err
			}
			value, err := readString(file)
			if err != nil {
				return err
			}

			expiration := time.Duration(-1)
			if currentExpiration != nil {
				expiration = currentExpiration.Sub(time.Now())
				if expiration <= 0 {
					// If the expiration is in the past, skip adding this key
					currentExpiration = nil
					continue
				}
			}

			store.Set(key, value, expiration)
			currentExpiration = nil // Reset expiration for the next key

		case 0xFF: // End of database section or file
			return nil

		default:
			return fmt.Errorf("unsupported value type: 0x%x", b)
		}
	}
}

func readSize(file *os.File) (uint64, error) {
	var b byte
	if err := binary.Read(file, binary.BigEndian, &b); err != nil {
		return 0, err
	}

	switch b >> 6 {
	case 0x00:
		return uint64(b & 0x3F), nil
	case 0x01:
		var b2 byte
		if err := binary.Read(file, binary.BigEndian, &b2); err != nil {
			return 0, err
		}
		return uint64(b&0x3F)<<8 | uint64(b2), nil
	case 0x02:
		var size uint32
		if err := binary.Read(file, binary.BigEndian, &size); err != nil {
			return uint64(size), nil
		}
	default:
		return 0, errors.New(fmt.Sprintf("invalid size encoding 0x%x", b))
	}
	return 0, nil
}

func readString(file *os.File) (string, error) {
	size, err := readSize(file)
	if err != nil {
		return "", err
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return "", err
	}

	return string(data), nil
}

func readUint64(file *os.File) (uint64, error) {
	var v uint64
	err := binary.Read(file, binary.LittleEndian, &v)
	return v, err
}

func readUint32(file *os.File) (uint32, error) {
	var v uint32
	err := binary.Read(file, binary.LittleEndian, &v)
	return v, err
}
