package carbonx

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

type metadata struct {
	aggType        uint32
	maxRetension   uint32
	xFilesFactor   uint32
	retentionCount uint32
}

func (m *metadata) readFrom(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &m.aggType)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &m.maxRetension)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &m.xFilesFactor)
	if err != nil {
		return err
	}
	return binary.Read(r, binary.BigEndian, &m.retentionCount)
}

type retention struct {
	offset          uint32
	secondsPerPoint uint32
	numberOfPoints  uint32
}

func (rt *retention) readFrom(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &rt.offset)
	if err != nil {
		return err
	}
	err = binary.Read(r, binary.BigEndian, &rt.secondsPerPoint)
	if err != nil {
		return err
	}
	return binary.Read(r, binary.BigEndian, &rt.numberOfPoints)
}

type dataPoint struct {
	interval uint32
	value    float64
}

func (p *dataPoint) readFrom(r io.Reader) error {
	err := binary.Read(r, binary.BigEndian, &p.interval)
	if err != nil {
		return err
	}
	v, err := readFloat64From(r)
	if err != nil {
		return err
	}
	p.value = v
	return nil
}

func readFloat64From(r io.Reader) (float64, error) {
	var intVal uint64
	err := binary.Read(r, binary.BigEndian, &intVal)
	if err != nil {
		return math.NaN(), err
	}
	return math.Float64frombits(intVal), nil
}

func dumpWhisper(filename string) error {
	return dumpWhisperTo(filename, os.Stdout)
}

func dumpWhisperTo(filename string, w io.Writer) error {
	fmt.Fprintf(w, "filename=%s\n", filename)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	m := &metadata{}
	err = m.readFrom(f)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "metadata=%+v\n", *m)

	retentions := make([]retention, m.retentionCount)
	for i := 0; i < len(retentions); i++ {
		err = retentions[i].readFrom(f)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "i=%d, retention=%+v\n", i, retentions[i])
	}
	dataPoints := make([][]dataPoint, len(retentions))
	for i := 0; i < len(retentions); i++ {
		dataPoints[i] = make([]dataPoint, retentions[i].numberOfPoints)
		for j := 0; j < int(retentions[i].numberOfPoints); j++ {
			err = dataPoints[i][j].readFrom(f)
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "i=%d, j=%d, interval=%d(%s), value=%v\n", i, j,
				dataPoints[i][j].interval,
				time.Unix(int64(dataPoints[i][j].interval), 0).Format("2006-01-02 15:04:05"),
				dataPoints[i][j].value)
		}
	}
	return nil
}
