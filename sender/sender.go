package sender

type Sender interface {
	Send(messages []Message) error
}
