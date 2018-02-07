package sender

import (
	"net"
	"time"

	"github.com/hnakamur/netutil"
	"github.com/lomik/go-carbon/helper/carbonpb"
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
	err := s.conn.Close()
	if err != nil {
		return err
	}
	s.conn = nil
	return nil
}

func (s *TCPSender) Send(metrics []*carbonpb.Metric) error {
	return retry.Do(func() error {
		data, err := s.marshaler.Marshal(metrics)
		if err != nil {
			return err
		}
		_, err = s.conn.Write(data)
		return err
	}, s.retryConfig.SendAttempts, s.retryConfig.SendSleepTime)
}

func (s *TCPSender) ConnectSendClose(metrics []*carbonpb.Metric) error {
	err := s.Connect()
	if err != nil {
		return err
	}
	defer s.Close()

	return s.Send(metrics)
}
