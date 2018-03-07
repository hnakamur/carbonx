package carbonx

import (
	"fmt"
	"math"
	"time"

	"github.com/hnakamur/carbonx/carbonpb"
)

type Merger struct {
	src  *Client
	dest *Client
}

func NewMerger(src, dest *Client) *Merger {
	return &Merger{
		src:  src,
		dest: dest,
	}
}

func (m *Merger) Diff(metric string, from, until time.Time) ([][]carbonpb.Point, error) {
	srcData, err := m.src.FetchData(metric, from, until)
	if err != nil {
		return nil, err
	}
	destData, err := m.dest.FetchData(metric, from, until)
	if err != nil {
		return nil, err
	}

	if srcData.StartTime != destData.StartTime {
		return nil, fmt.Errorf("unmatched fetchResponse StartTime, metric=%s, srcStartTime=%d, destStartTime=%d",
			metric, srcData.StartTime, destData.StartTime)
	}
	if srcData.StopTime != destData.StopTime {
		return nil, fmt.Errorf("unmatched fetchResponse StopTime, metric=%s, srcStopTime=%d, destStopTime=%d",
			metric, srcData.StopTime, destData.StopTime)
	}
	if srcData.StepTime != destData.StepTime {
		return nil, fmt.Errorf("unmatched fetchResponse StepTime, metric=%s, srcStepTime=%d, destStepTime=%d",
			metric, srcData.StepTime, destData.StepTime)
	}

	points := make([][]carbonpb.Point, 2)
	for i := range srcData.Values {
		srcAbsent := srcData.IsAbsent[i]
		destAbsent := destData.IsAbsent[i]

		var srcValue float64
		if srcAbsent {
			srcValue = math.NaN()
		} else {
			srcValue = srcData.Values[i]
		}

		var destValue float64
		if destAbsent {
			destValue = math.NaN()
		} else {
			destValue = destData.Values[i]
		}

		if (srcAbsent && destAbsent) || (!srcAbsent && !destAbsent && srcValue == destValue) {
			continue
		}

		ts := uint32(srcData.StartTime) + uint32(i)*uint32(destData.StepTime)
		points[0] = append(points[0], carbonpb.Point{
			Timestamp: ts,
			Value:     srcValue,
		})
		points[1] = append(points[1], carbonpb.Point{
			Timestamp: ts,
			Value:     destValue,
		})
	}
	return points, nil
}
