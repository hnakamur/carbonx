package sender

import (
	"net"
	"strconv"
)

type tcpSender struct {
	sendToAddress string
}

func NewTCP(sendToAddress string) (Sender, error) {
	_, _, err := net.SplitHostPort(sendToAddress)
	if err != nil {
		return nil, err
	}
	return &tcpSender{
		sendToAddress: sendToAddress,
	}, nil
}

func (s *tcpSender) Send(messages []Message) error {
	conn, err := net.Dial("tcp", s.sendToAddress)
	if err != nil {
		return err
	}
	defer conn.Close()

	var data []byte
	for _, m := range messages {
		for _, p := range m.Points {
			data = append(data, m.Name...)
			data = append(data, ' ')
			data = strconv.AppendFloat(data, p.Value, 'g', -1, 64)
			data = append(data, ' ')
			data = strconv.AppendInt(data, p.Timestamp, 10)
			data = append(data, '\n')
		}
	}
	_, err = conn.Write(data)
	return err
}
