package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ReplicaInfo struct {
	role               string
	replicationId      string
	replicationOffset  int
	replicaConnections []net.Conn
	numAck             int
	ackChan            chan struct{} // Channel to signal ACK updates
	mu                 sync.Mutex
}

func (ri *ReplicaInfo) incrementAck() {
	ri.mu.Lock()
	ri.numAck++
	ri.mu.Unlock()
	ri.ackChan <- struct{}{}
}

func (ri *ReplicaInfo) getNumAck() int {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	return ri.numAck

}

func (ri *ReplicaInfo) setNumAck(val int) {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	ri.numAck = val
}

func (ri *ReplicaInfo) String() string {
	return fmt.Sprintf(
		"role:%s\nmaster_replid:%s\nmaster_repl_offset:%d",
		ri.role, ri.replicationId, ri.replicationOffset,
	)
}

func main() {
	port := flag.Int("port", 6379, "Port to bind the server to.")
	replicaOf := flag.String("replicaof", "", "Specify the master host for replication")
	flag.Parse()
	args := flag.Args()

	replicaInfo := ReplicaInfo{ackChan: make(chan struct{}, 10)}
	storage := NewStorage()

	if *replicaOf != "" {
		replicaInfo.role = "slave"
		replicaInfo.replicationOffset = 0
		masterPort := args[0]
		go handleHandshake(storage, &replicaInfo, masterPort, strconv.Itoa(*port))
	} else {
		replicaInfo.role = "master"
		replicaInfo.replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		replicaInfo.replicationOffset = 0
	}

	acceptConnections(storage, &replicaInfo, port)
}

func acceptConnections(storage *Storage, replicaInfo *ReplicaInfo, port *int) {
	address := fmt.Sprintf("0.0.0.0:%d", *port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		logErrorAndExit(fmt.Sprintf("Failed to bind to port %d", *port), err)
	}
	fmt.Printf("Redis Server is listening on %s\n", address)
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		reader := bufio.NewReader(conn)
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn, reader, storage, replicaInfo)
	}
}

func handleConnection(conn net.Conn, reader *bufio.Reader, storage *Storage, replicaInfo *ReplicaInfo) {
	defer conn.Close()

	for {
		resp, err := DecodeRESP(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error decoding RESP: %v\n", err)
			}
			continue
		}
		executeCommand(conn, resp, storage, replicaInfo)
	}
}

func executeCommand(conn net.Conn, resp RESP, storage *Storage, replicaInfo *ReplicaInfo) {
	command := resp.List[0].GetString()
	args := resp.List[1:]

	numBytes := len(EncodeArray(resp.GetArray()))

	curReplicationOffset := replicaInfo.replicationOffset
	switch strings.ToLower(command) {
	case "ping":
		if replicaInfo.role == "master" {
			sendResponse(conn, "+PONG\r\n")
		} else {
			replicaInfo.replicationOffset += numBytes
		}
	case "echo":
		echoResponse(conn, args)
	case "set":
		replicaInfo.replicationOffset += numBytes
		if replicaInfo.role == "slave" {
			fmt.Println("Replica is setting key", args[0].GetString())
		}
		setKey(storage, args)
		if replicaInfo.role == "master" {
			handlePropagation(command, args, replicaInfo)
			sendResponse(conn, "+OK\r\n")
		}
	case "replconf":
		fmt.Println(replicaInfo.role)
		fmt.Println(command)
		fmt.Println(args[0].GetString())
		fmt.Println(args[1].GetString())
		if args[0].GetString() == "GETACK" {
			fmt.Println("Replica gets getack")
			conn.Write(EncodeBulkStringsToArray([]string{"REPLCONF", "ACK", strconv.Itoa(curReplicationOffset)}))
			replicaInfo.replicationOffset += numBytes
		} else if args[0].GetString() == "ACK" {
			fmt.Println("Master got ACK, increasing numAck by 1")
			replicaInfo.incrementAck()
		} else {
			sendResponse(conn, "+OK\r\n")
		}
	case "psync":
		replicaInfo.replicaConnections = append(replicaInfo.replicaConnections, conn)
		sendResponse(conn, fmt.Sprintf("+FULLRESYNC %s %s\r\n", replicaInfo.replicationId, strconv.Itoa(replicaInfo.replicationOffset)))
		emptyRdbContent := ConvertRdbFileToByteArr("app/empty_rdb.hex")
		conn.Write(emptyRdbContent)
	case "get":
		getKey(conn, storage, args)
	case "info":
		sendInfo(conn, replicaInfo)
	case "wait":
		if replicaInfo.replicationOffset == 0 {
			fmt.Println("Master has not propagated any set commands.")
			fmt.Println("Responding to client the num of replicas:", len(replicaInfo.replicaConnections))
			sendResponse(conn, fmt.Sprintf(":%d\r\n", len(replicaInfo.replicaConnections)))
			break
		}
		nMinimumReplica, _ := strconv.Atoi(args[0].GetString())
		timeout, _ := strconv.Atoi(args[1].GetString())
		deadline := time.Now().Add(time.Millisecond * time.Duration(timeout))
		ticker := time.NewTicker(10 * time.Millisecond) // Check interval
		defer ticker.Stop()
		replicaInfo.setNumAck(0)

		getAckBytes := EncodeBulkStringsToArray([]string{"REPLCONF", "GETACK", "*"})
		replicaInfo.replicationOffset += len(getAckBytes)

		for _, conn := range replicaInfo.replicaConnections {
			conn.Write(getAckBytes)
		}

		done := false
		for !done {
			select {
			case <-replicaInfo.ackChan:
				ackCount := replicaInfo.getNumAck()
				if ackCount >= nMinimumReplica {
					done = true
				}
			case <-ticker.C:
				if time.Now().After(deadline) {
					done = true
				}
			}
		}
		fmt.Println("Responding to client the num of ack:", replicaInfo.getNumAck())
		sendResponse(conn, fmt.Sprintf(":%d\r\n", replicaInfo.getNumAck()))
	default:
		fmt.Println("Unknown command")
	}
}

func handlePropagation(command string, args []RESP, replicaInfo *ReplicaInfo) {
	for _, conn := range replicaInfo.replicaConnections {
		toPropagate := append([]RESP{{Type: BulkString, Bytes: []byte(command)}}, args...)
		conn.Write(EncodeArray(toPropagate))
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
