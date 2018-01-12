package sender

import (
	"encoding/binary"
	"net"

	"github.com/lomik/graphite-pickle/framing"
)

type TCP struct {
	sendToAddress string
	encoder       encoder
}

func NewTCP(sendToAddress string, encoding Encoding) (*TCP, error) {
	_, _, err := net.SplitHostPort(sendToAddress)
	if err != nil {
		return nil, err
	}
	return &TCP{
		sendToAddress: sendToAddress,
		encoder:       newEncoder(encoding),
	}, nil
}

func (s *TCP) Send(messages []Message) error {
	conn, err := net.Dial("tcp", s.sendToAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	framedConn, err := framing.NewConn(conn, byte(4), binary.BigEndian)
	if err != nil {
		return err
	}

	data, err := s.encoder.Encode(messages)
	if err != nil {
		return err
	}

	_, err = framedConn.Write(data)
	return err
}
