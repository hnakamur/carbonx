package sender

import (
	"encoding/binary"
	"net"

	pickle "github.com/lomik/graphite-pickle"
	"github.com/lomik/graphite-pickle/framing"
)

type pickleSender struct {
	sendToAddress string
}

func NewPickle(sendToAddress string) (Sender, error) {
	_, _, err := net.SplitHostPort(sendToAddress)
	if err != nil {
		return nil, err
	}
	return &pickleSender{
		sendToAddress: sendToAddress,
	}, nil
}

func (s *pickleSender) Send(messages []Message) error {
	conn, err := net.Dial("tcp", s.sendToAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	framedConn, err := framing.NewConn(conn, byte(4), binary.BigEndian)
	if err != nil {
		return err
	}

	data, err := pickle.MarshalMessages(messages)
	if err != nil {
		return err
	}

	_, err = framedConn.Write(data)
	return err
}
