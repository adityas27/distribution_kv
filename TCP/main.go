package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)


type Server struct {
	mu      sync.Mutex   // to lock and unlock
	counter int          // starting with something basic as counter will change it to byte in next stage of implementation
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	fmt.Println("Listening on", addr)

	for {
		conn, err := ln.Accept()
		fmt.Println(conn)
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Client connected:", conn.RemoteAddr())
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		response := s.parseCommand(line)

		_, err := fmt.Fprintln(conn, response)
		if err != nil {
			return
		}
		if response == "BYE" {
			return
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("read error:", err)
	}
}

func (s *Server) parseCommand(line string) string {
	fields := strings.Fields(line)

	if len(fields) == 0 {
		return "ERROR empty command"
	}

	cmd := strings.ToUpper(fields[0])
	args := fields[1:]

	switch cmd {

	case "PING":
		return "PONG"

	case "ECHO":
		return strings.Join(args, " ")

	case "QUIT":
		return "BYE"

	default:
		return "ERROR unknown command"
	}
}

func main() {
	server := NewServer()

	if err := server.Start(":9000"); err != nil {
		panic(err)
	}
}	