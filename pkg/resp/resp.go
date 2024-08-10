package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func EncodeRESPString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func EncodeRESPArray(elements []string) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(elements)))
	for _, elem := range elements {
		sb.WriteString(EncodeRESPString(elem))
	}
	return sb.String()
}

func EncodeRESPInteger(i int64) string {
	return fmt.Sprintf(":%d\r\n", i)
}

func EncodeRESPSimpleString(s string) string {
	return fmt.Sprintf("+%s\r\n", s)
}

func EncodeRESPNull() string {
	return "$-1\r\n"
}

func EncodeRESPError(err string) string {
	return fmt.Sprintf("-ERR %s\r\n", err)
}

func ParseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return nil, nil
	}

	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("invalid RESP command: %s", line)
	}

	numElements, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid number of elements")
	}

	var result []string
	for i := 0; i < numElements; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("invalid RESP bulk string")
		}

		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length")
		}

		buf := make([]byte, length+2)
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return nil, err
		}

		result = append(result, string(buf[:length]))
	}

	return result, nil
}
