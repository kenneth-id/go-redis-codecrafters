package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	port := flag.Int("port", 6379, "Port to bind the server to.")
	flag.Parse()

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		fmt.Printf("Failed to bind to port %d: %v\n", port, err)
		os.Exit(1)
	}

	defer listener.Close()
	storage := NewStorage()
	fmt.Printf("Redis Server is listening on port %d \n", *port)
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
			if len(args) > 2 {
				durationString := fmt.Sprintf("%sms", args[3].GetString())
				duration, err := time.ParseDuration(durationString)
				if err != nil {
					fmt.Println("Error parsing time duration", err)
				}
				storage.Set(key, val, duration)
			} else {
				storage.Set(key, val, 0)

			}
			okReply := []byte("+OK\r\n")
			conn.Write(okReply)
		case "get":
			key := args[0].GetString()
			val, ok := storage.Get(key)
			fmt.Println(val)
			if !ok {
				conn.Write([]byte("$-1\r\n"))
				continue
			}
			n := len(val)
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", n, val)))
		}
	}
}
