package sender

import (
	"encoding/binary"
	"strconv"

	pbc "github.com/lomik/go-carbon/helper/carbonpb"
)

type MetricsMarshaler interface {
	Marshal(metrics []*pbc.Metric) ([]byte, error)
}

type TextMetricsMarshaler struct{}

func NewTextMetricsMarshaler() *TextMetricsMarshaler {
	return &TextMetricsMarshaler{}
}

func (m *TextMetricsMarshaler) Marshal(metrics []*pbc.Metric) ([]byte, error) {
	var data []byte
	for _, m := range metrics {
		for _, p := range m.Points {
			data = append(data, m.Metric...)
			data = append(data, ' ')
			data = strconv.AppendFloat(data, p.Value, 'g', -1, 64)
			data = append(data, ' ')
			data = strconv.AppendInt(data, int64(p.Timestamp), 10)
			data = append(data, '\n')
		}
	}
	return data, nil
}

type Protobuf3MetricsMarshaler struct{}

func NewProtobuf3MetricsMarshaler() *Protobuf3MetricsMarshaler {
	return &Protobuf3MetricsMarshaler{}
}

func (m *Protobuf3MetricsMarshaler) Marshal(metrics []*pbc.Metric) ([]byte, error) {
	payload := pbc.Payload{Metrics: metrics}
	data, err := payload.Marshal()
	if err != nil {
		return nil, err
	}

	length := len(data)
	const uint32Len = 4
	var extend [uint32Len]byte
	data = append(data, extend[:]...)
	copy(data[uint32Len:], data[:length])
	binary.BigEndian.PutUint32(data[:uint32Len], uint32(length))
	return data, nil
}
