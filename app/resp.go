package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

type Type byte

const CLRF = "\r\n"

const (
	SimpleString Type = '+'
	BulkString   Type = '$'
	Array        Type = '*'
	Integer      Type = ':'
)

type RESP struct {
	Type  Type
	Bytes []byte
	List  []RESP
}

// String converts RESP  to a string.
//
// If RESP  cannot be converted, an empty string is returned.
func (r *RESP) GetString() string {
	if r.Type == BulkString || r.Type == SimpleString {
		return string(r.Bytes)
	}

	return ""
}

// Array converts RESP  to an array.
//
// If RESP  cannot be converted, an empty array is returned.
func (r *RESP) GetArray() []RESP {
	if r.Type == Array {
		return r.List
	}

	return []RESP{}
}

// DecodeRESP parses a RESP message and returns a RedisValue
func DecodeRESP(byteStream *bufio.Reader) (RESP, error) {
	dataTypeByte, err := byteStream.ReadByte()
	if err != nil {
		return RESP{}, err
	}

	switch string(dataTypeByte) {
	case "+":
		return decodeSimpleString(byteStream)
	case "$":
		return decodeBulkString(byteStream)
	case "*":
		return decodeArray(byteStream)
	default:
		fmt.Println("Undecodeable RESP from connection")
	}

	return RESP{}, fmt.Errorf("invalid RESP data type byte: %s", string(dataTypeByte))
}

func decodeSimpleString(byteStream *bufio.Reader) (RESP, error) {
	readBytes, err := readUntilCRLF(byteStream)
	if err != nil {
		return RESP{}, err
	}

	return RESP{
		Type:  SimpleString,
		Bytes: readBytes,
	}, nil
}

func decodeBulkString(byteStream *bufio.Reader) (RESP, error) {
	readBytesForCount, err := readUntilCRLF(byteStream)
	if err != nil {
		return RESP{}, fmt.Errorf("failed to read bulk string length: %s", err)
	}

	count, err := strconv.Atoi(string(readBytesForCount))
	if err != nil {
		return RESP{}, fmt.Errorf("failed to parse bulk string length: %s", err)
	}

	readBytes := make([]byte, count+2)

	if _, err := io.ReadFull(byteStream, readBytes); err != nil {
		return RESP{}, fmt.Errorf("failed to read bulk string contents: %s", err)
	}

	return RESP{
		Type:  BulkString,
		Bytes: readBytes[:count],
	}, nil
}

func decodeArray(byteStream *bufio.Reader) (RESP, error) {
	readBytesForCount, err := readUntilCRLF(byteStream)
	if err != nil {
		return RESP{}, fmt.Errorf("failed to read bulk string length: %s", err)
	}

	count, err := strconv.Atoi(string(readBytesForCount))
	if err != nil {
		return RESP{}, fmt.Errorf("failed to parse bulk string length: %s", err)
	}

	array := []RESP{}

	for i := 1; i <= count; i++ {
		value, err := DecodeRESP(byteStream)
		if err != nil {
			return RESP{}, err
		}

		array = append(array, value)
	}

	return RESP{
		Type: Array,
		List: array,
	}, nil
}

func readUntilCRLF(byteStream *bufio.Reader) ([]byte, error) {
	readBytes := []byte{}

	for {
		b, err := byteStream.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		readBytes = append(readBytes, b...)
		if len(readBytes) >= 2 && readBytes[len(readBytes)-2] == '\r' {
			break
		}
	}

	return readBytes[:len(readBytes)-2], nil
}

func EncodeBulkString(data []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte('$')
	buf.WriteString(strconv.Itoa(len(data)))
	buf.WriteString(CLRF)
	buf.Write(data)
	buf.WriteString(CLRF)
	return buf.Bytes()
}

func EncodeArray(array []RESP) []byte {
	var buf bytes.Buffer
	buf.WriteByte('*')
	buf.WriteString(strconv.Itoa(len(array)))
	buf.WriteString(CLRF)
	for _, resp := range array {
		if resp.Type == BulkString {
			buf.Write(EncodeBulkString(resp.Bytes))
		}
	}
	return buf.Bytes()
}

func EncodeBulkStringsToArray(strings []string) []byte {
	resps := make([]RESP, 0, len(strings))

	for _, str := range strings {
		resps = append(resps, RESP{
			Type:  BulkString,
			Bytes: []byte(str),
		})
	}

	return EncodeArray(resps)
}
