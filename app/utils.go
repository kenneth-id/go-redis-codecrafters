package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
)

func handleHandshake(storage *Storage, replicaInfo *ReplicaInfo, masterPort string, slavePort string) {
	address := fmt.Sprintf("0.0.0.0:%s", masterPort)
	conn, err := net.Dial("tcp", address)
	reader := bufio.NewReader(conn)

	if err != nil {
		fmt.Printf("Failed to dial master at port %s\n", masterPort)
	}
	// defer conn.Close()

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

	// Read RDB content
	emptyRdbContent := ConvertRdbFileToByteArr("app/empty_rdb.hex")
	emptyRdb := make([]byte, len(emptyRdbContent))
	n, err := io.ReadFull(reader, emptyRdb)
	if err != nil {
		fmt.Println("Error decoding RDB file from master")
	}
	fmt.Println("Length of empty RDB", n)
	fmt.Printf("%q\n", emptyRdb)

	handleConnection(conn, storage, replicaInfo)
}

func ConvertRdbFileToByteArr(filePath string) []byte {
	// Read the contents of the file
	fileContents, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return nil
	}
	// Encode the file contents to hexadecimal
	hexDecoded := make([]byte, hex.DecodedLen(len(fileContents)))
	_, err = hex.Decode(hexDecoded, fileContents)
	if err != nil {
		fmt.Println("Error decoding:", err)
		return nil
	}
	// Calculate the length of the hexadecimal decoded string
	hexLength := len(hexDecoded)
	// Format the data as bytes array
	formattedData := fmt.Sprintf("$%d\r\n%s", hexLength, hexDecoded)
	// Convert the formatted data to a byte slice
	dataBytes := []byte(formattedData)
	return dataBytes
}
