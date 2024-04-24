package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func handleHandshake(masterPort string, slavePort string) {
	address := fmt.Sprintf("0.0.0.0:%s", masterPort)
	conn, err := net.Dial("tcp", address)
	reader := bufio.NewReader(conn)

	if err != nil {
		logErrorAndExit(fmt.Sprintf("Failed to dial master at port %s", masterPort), err)
	}
	defer conn.Close()

	pingRESP := RESP{
		Type:  BulkString,
		Bytes: []byte("ping"),
	}
	pingArrayRESP := []RESP{pingRESP}
	conn.Write(EncodeArray(pingArrayRESP))

	resp1, err := DecodeRESP(reader)
	if err != nil {
		fmt.Println("Error decoding ping handshake response from master:", err)
	}
	if resp1.GetString() != "PONG" {
		fmt.Println("Received invalid response from master:", resp1.GetString())
		os.Exit(1)
	}

	conn.Write(EncodeBulkStringsToArray([]string{"REPLCONF", "listening-port", slavePort}))

	resp2, err := DecodeRESP(reader)
	if err != nil {
		fmt.Println("Error decoding first REPLCONF handshake response from master")
	}
	if resp2.GetString() != "OK" {
		fmt.Println("Received invalid response from master:", resp2.GetString())
		os.Exit(1)
	}

	conn.Write(EncodeBulkStringsToArray([]string{"REPLCONF", "capa", "psync2"}))

	resp3, err := DecodeRESP(reader)
	if err != nil {
		fmt.Println("Error decoding second REPLCONF handshake response from master")
	}
	if resp3.GetString() != "OK" {
		fmt.Println("Received invalid response from master:", resp3.GetString())
		os.Exit(1)
	}

	conn.Write(EncodeBulkStringsToArray([]string{"PSYNC", "?", "-1"}))

	resp4, err := DecodeRESP(reader)
	if err != nil {
		fmt.Println("Error decoding handshake response from master")
	}
	fmt.Println(resp4.GetString())
	// if resp2.GetString() != "OK" {
	// 	fmt.Println("Received invalid response from master:", resp4.GetString())
	// 	os.Exit(1)
}
