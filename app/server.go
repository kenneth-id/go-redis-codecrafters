package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer listener.Close()
	storage := NewStorage()
	fmt.Println("Redis Server is listening on port 6379")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn, storage)
	}
}

func handleConnection(conn net.Conn, storage *Storage) {
	defer conn.Close()

	for {
		reader := bufio.NewReader(conn)
		resp, err := DecodeRESP(reader)
		if err != nil {
			fmt.Println("Error decoding RESP", err.Error())
			return
		}
		command := resp.List[0].GetString()
		args := resp.List[1:]

		switch command {
		case "ping":
			pongReply := []byte("+PONG\r\n")
			conn.Write(pongReply)
		case "echo":
			str := args[0].GetString()
			n := len(str)
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", n, str)))
		case "set":
			key := args[0].GetString()
			val := args[1].GetString()
			storage.Set(key, val)
			okReply := []byte("+OK\r\n")
			conn.Write(okReply)
		case "get":
			key := args[0].GetString()
			val, ok := storage.Get(key)
			fmt.Println(val)
			if !ok {
				conn.Write([]byte("-1\r\n"))
			}
			n := len(val)
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", n, val)))
		}
	}
}
