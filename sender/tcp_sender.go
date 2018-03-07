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

func (s *TCPSender) Send(metrics []*carbonpb.Metric) error {
	conn, err := net.Dial("tcp", s.sendToAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	data, err := s.marshaler.Marshal(metrics)
	if err != nil {
		return err
	}
	_, err = conn.Write(data)
	return err
}
