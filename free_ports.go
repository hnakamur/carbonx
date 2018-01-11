package carbonx

import "net"

// GetFreePorts returns free TCP ports of the specified count.
func GetFreePorts(count int) ([]int, error) {
	// Based on https://github.com/phayes/freeport/blob/master/freeport.go
	var ports []int
	for i := 0; i < count; i++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return nil, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		defer l.Close()
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}
