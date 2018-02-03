package sender

import (
	"net"
	"time"

	"github.com/hnakamur/carbonx/pb"
	"github.com/hnakamur/netutil"
	retry "github.com/rafaeljesus/retry-go"
)

type RetryConfig struct {
	ConnectAttempts  int
	ConnectSleepTime time.Duration
	SendAttempts     int
	SendSleepTime    time.Duration
}

type TCPSender struct {
	sendToAddress string
	marshaler     MetricsMarshaler
	retryConfig   RetryConfig

	conn net.Conn
}

func NewTCPSender(sendToAddress string, marshaler MetricsMarshaler, retryConfig RetryConfig) (*TCPSender, error) {
	_, _, err := netutil.SplitHostPort(sendToAddress)
	if err != nil {
		return nil, err
	}
	return &TCPSender{
		sendToAddress: sendToAddress,
		marshaler:     marshaler,
		retryConfig:   retryConfig,
	}, nil
}

func (s *TCPSender) Connect() error {
	return retry.Do(func() error {
		conn, err := net.Dial("tcp", s.sendToAddress)
		if err != nil {
			return err
		}
		s.conn = conn
		return nil
	}, s.retryConfig.ConnectAttempts, s.retryConfig.ConnectSleepTime)
}

func (s *TCPSender) Close() error {
	return s.conn.Close()
}

func (s *TCPSender) Send(metrics []*pb.Metric) error {
	return retry.Do(func() error {
		data, err := s.marshaler.Marshal(metrics)
		if err != nil {
			return err
		}
		_, err = s.conn.Write(data)
		return err
	}, s.retryConfig.SendAttempts, s.retryConfig.SendSleepTime)
}

func (s *TCPSender) ConnectSendClose(metrics []*pb.Metric) error {
	err := s.Connect()
	if err != nil {
		return err
	}
	defer s.Close()

	return s.Send(metrics)
}
