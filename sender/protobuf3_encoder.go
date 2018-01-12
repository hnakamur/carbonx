package sender

type protobuf3Encoder struct{}

func (p *protobuf3Encoder) Encode(messages []Message) ([]byte, error) {
	return nil, nil
}

func (p *protobuf3Encoder) ContentType() string {
	return "application/protobuf"
}
