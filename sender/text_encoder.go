package sender

import "strconv"

type textEncoder struct{}

func (p *textEncoder) Encode(messages []Message) ([]byte, error) {
	var buf []byte
	for _, m := range messages {
		for _, p := range m.Points {
			buf = append(buf, m.Name...)
			buf = append(buf, ' ')
			buf = strconv.AppendFloat(buf, p.Value, 'g', -1, 64)
			buf = append(buf, ' ')
			buf = strconv.AppendInt(buf, p.Timestamp, 10)
			buf = append(buf, '\n')
		}
	}
	return buf, nil
}

func (p *textEncoder) ContentType() string {
	return "plain/text"
}
