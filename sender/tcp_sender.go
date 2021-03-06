package sender

import (
	"net"

	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/netutil"
)

type TCPSender struct {
	sendToAddress string
	marshaler     MetricsMarshaler

	conn net.Conn
}

func NewTCPSender(sendToAddress string, marshaler MetricsMarshaler) (*TCPSender, error) {
	_, _, err := netutil.SplitHostPort(sendToAddress)
	if err != nil {
		return nil, err
	}
	return &TCPSender{
		sendToAddress: sendToAddress,
		marshaler:     marshaler,
	}, nil
}

func (s *TCPSender) Connect() error {
	conn, err := net.Dial("tcp", s.sendToAddress)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

func (s *TCPSender) Close() error {
	err := s.conn.Close()
	if err != nil {
		return err
	}
	s.conn = nil
	return nil
}

func (s *TCPSender) Send(metrics []*carbonpb.Metric) error {
	data, err := s.marshaler.Marshal(metrics)
	if err != nil {
		return err
	}
	_, err = s.conn.Write(data)
	return err
}

func (s *TCPSender) ConnectSendClose(metrics []*carbonpb.Metric) error {
	err := s.Connect()
	if err != nil {
		return err
	}
	defer s.Close()

	return s.Send(metrics)
}
