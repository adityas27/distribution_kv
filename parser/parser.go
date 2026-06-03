package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type Command struct {
	Name  string
	Key   string
	Value string
	TTL   int
}

func Parse(line string) (*Command, error) {
	fields := strings.Fields(strings.TrimSpace(line))

	if len(fields) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	switch strings.ToUpper(fields[0]) {

	case "PING":
		return &Command{Name: "PING"}, nil

	case "GET":
		if len(fields) != 2 {
			return nil, fmt.Errorf("usage: GET <key>")
		}

		return &Command{
			Name: "GET",
			Key:  fields[1],
		}, nil

	case "DELETE":
		if len(fields) != 2 {
			return nil, fmt.Errorf("usage: DELETE <key>")
		}

		return &Command{
			Name: "DELETE",
			Key:  fields[1],
		}, nil

	case "SET":

		if len(fields) < 3 {
			return nil, fmt.Errorf("usage: SET <key> <value> [EX seconds]")
		}

		cmd := &Command{
			Name:  "SET",
			Key:   fields[1],
			Value: fields[2],
		}

		if len(fields) == 5 {
			if strings.ToUpper(fields[3]) != "EX" {
				return nil, fmt.Errorf("expected EX")
			}

			ttl, err := strconv.Atoi(fields[4])
			if err != nil {
				return nil, fmt.Errorf("invalid ttl")
			}

			cmd.TTL = ttl
		}

		return cmd, nil

	default:
		return nil, fmt.Errorf("unknown command")
	}
}