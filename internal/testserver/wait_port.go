package testserver

import (
	"net"
	"time"

	retry "github.com/rafaeljesus/retry-go"
)

func WaitTCPPortConnectable(address string, attempts int, sleepTime time.Duration) error {
	return retry.Do(func() error {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return err
		}
		conn.Close()
		return nil
	}, attempts, sleepTime)
}
