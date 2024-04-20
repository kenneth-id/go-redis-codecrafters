package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

type ReplicaInfo struct {
	role              string
	replicationId     string
	replicationOffset int
}

func (ri *ReplicaInfo) String() string {
	return fmt.Sprintf(
		"role:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
		ri.role, ri.replicationId, ri.replicationOffset,
	)
}

func main() {
	port := flag.Int("port", 6379, "Port to bind the server to.")
	replicaOf := flag.String("replicaof", "master", "Specify the master host for replication")

	flag.Parse()

	replicaInfo := ReplicaInfo{}
	args := flag.Args()
	if *replicaOf != "master" && len(args) == 1 {
		replicaInfo.role = "slave"
		masterPort := args[0]
		address := fmt.Sprintf("0.0.0.0:%s", masterPort)
		conn, err := net.Dial("tcp", address)
		if err != nil {
			logErrorAndExit(fmt.Sprintf("Failed to dial master at port %s", masterPort), err)
		}
		pingRESP := RESP{
			Type:  BulkString,
			Bytes: []byte("ping"),
		}
		resps := []RESP{pingRESP}
		conn.Write(EncodeArray(resps))
	} else {
		replicaInfo.role = "master"
		replicaInfo.replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		replicaInfo.replicationOffset = 0
	}

	address := fmt.Sprintf("0.0.0.0:%d", *port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		logErrorAndExit(fmt.Sprintf("Failed to bind to port %d", *port), err)
	}
	defer listener.Close()

	storage := NewStorage()
	fmt.Printf("Redis Server is listening on %s\n", address)

	acceptConnections(listener, storage, &replicaInfo)
}

func acceptConnections(listener net.Listener, storage *Storage, replicaInfo *ReplicaInfo) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn, storage, replicaInfo)
	}
}

func handleConnection(conn net.Conn, storage *Storage, replicaInfo *ReplicaInfo) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		resp, err := DecodeRESP(reader)
		if err != nil {
			fmt.Printf("Error decoding RESP: %v\n", err)
			return
		}
		executeCommand(conn, resp, storage, replicaInfo)
	}
}

func executeCommand(conn net.Conn, resp RESP, storage *Storage, replicaInfo *ReplicaInfo) {
	command := resp.List[0].GetString()
	args := resp.List[1:]
	switch strings.ToLower(command) {
	case "ping":
		sendResponse(conn, "+PONG\r\n")
	case "echo":
		echoResponse(conn, args)
	case "set":
		setKey(storage, args)
		sendResponse(conn, "+OK\r\n")
	case "get":
		getKey(conn, storage, args)
	case "info":
		sendInfo(conn, replicaInfo)
	}
}

func sendInfo(conn net.Conn, replicaInfo *ReplicaInfo) {
	info := replicaInfo.String()
	n := len(info)
	sendResponse(conn, fmt.Sprintf("$%d\r\n%s\r\n", n, info))
}

func echoResponse(conn net.Conn, args []RESP) {
	str := args[0].GetString()
	n := len(str)
	sendResponse(conn, fmt.Sprintf("$%d\r\n%s\r\n", n, str))
}

func setKey(storage *Storage, args []RESP) {
	key := args[0].GetString()
	val := args[1].GetString()
	if len(args) > 2 {
		setKeyWithExpiry(storage, args, key, val)
	} else {
		storage.Set(key, val, 0)
	}
}

func setKeyWithExpiry(storage *Storage, args []RESP, key, value string) {
	durationString := fmt.Sprintf("%sms", args[3].GetString())
	duration, err := time.ParseDuration(durationString)
	if err != nil {
		fmt.Printf("Error parsing time duration: %v\n", err)
		return
	}
	storage.Set(key, value, duration)
}

func getKey(conn net.Conn, storage *Storage, args []RESP) {
	key := args[0].GetString()
	val, ok := storage.Get(key)
	if !ok {
		sendResponse(conn, "$-1\r\n")
		return
	}
	sendResponse(conn, fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
}

func sendResponse(conn net.Conn, message string) {
	conn.Write([]byte(message))
}

func logErrorAndExit(message string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", message, err)
	os.Exit(1)
}
