package main

import (
	"bufio"
	"fmt"
	"net"

	"tcp_test/parser"
	"tcp_test/storage"
)

type Server struct {
    cache *storage.Cache
}

func NewServer() *Server {
	return &Server{
		cache: storage.NewCache(),
	}
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
	if scanner.Err() != nil{
		fmt.Println(scanner.Err().Error())
	}
	for scanner.Scan() {
		line := scanner.Text()

		cmd, err := parser.Parse(line)
		if err != nil {
			fmt.Fprintln(conn, "ERROR", err.Error())
			continue
		}

		response := s.execute(cmd)

		_, err = fmt.Fprintln(conn, response)
		if err != nil {
			return
		}
	}
}

func (s *Server) execute(cmd *parser.Command) string {
	switch cmd.Name {

	case "PING":
		return "PONG"

	case "SET":
		s.cache.Set(cmd.Key, cmd.Value, cmd.TTL)
		return "OK"

	case "GET":
		value, ok := s.cache.Get(cmd.Key)
		if !ok {
			return "NULL"
		}
		return value

	
	case "DELETE":
		s.cache.Delete(cmd.Key)
		return "OK"

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