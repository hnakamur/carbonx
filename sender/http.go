package sender

type HTTP struct {
}

func NewHTTP(sendToAddress string, encoding Encoding) (*HTTP, error) {
	return nil, nil
}

func (s *HTTP) Send(messages []Message) error {
	return nil
}
