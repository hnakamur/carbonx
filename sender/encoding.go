package sender

type Encoding int

const (
	EncodingText = iota + 1
	EncodingPickle
	EncodingProtobuf3
)
