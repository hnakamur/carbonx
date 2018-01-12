package sender

type encoder interface {
	Encode(messages []Message) ([]byte, error)
	ContentType() string
}

func newEncoder(encoding Encoding) encoder {
	switch encoding {
	case EncodingText:
		return &textEncoder{}
	case EncodingPickle:
		return &pickleEncoder{}
	case EncodingProtobuf3:
		return &protobuf3Encoder{}
	default:
		panic("should not happen")
	}
}
