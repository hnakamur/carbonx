package sender

import pickle "github.com/lomik/graphite-pickle"

type pickleEncoder struct{}

func (p *pickleEncoder) Encode(messages []Message) ([]byte, error) {
	return pickle.MarshalMessages(messages)
}

func (p *pickleEncoder) ContentType() string {
	return "applihhtion/python-pickle"
}
