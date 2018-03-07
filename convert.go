package carbonx

import (
	"fmt"
	"math"

	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/carbonx/carbonzipperpb3"
)

func convertFetchedDataToPoints(r *carbonzipperpb3.FetchResponse) []carbonpb.Point {
	var points []carbonpb.Point
	for i, v := range r.Values {
		if r.IsAbsent[i] {
			v = math.NaN()
		}
		points = append(points, carbonpb.Point{
			Timestamp: uint32(r.StartTime) + uint32(i)*uint32(r.StepTime),
			Value:     v,
		})
	}
	return points
}

func convertFetchResponseToMetric(r *carbonzipperpb3.FetchResponse) *carbonpb.Metric {
	m := &carbonpb.Metric{
		Metric: r.Name,
	}
	for i, v := range r.Values {
		if r.IsAbsent[i] {
			continue
		}
		m.Points = append(m.Points, carbonpb.Point{
			Timestamp: uint32(r.StartTime) + uint32(i)*uint32(r.StepTime),
			Value:     v,
		})
	}
	return m
}

func convertFetchResponseToMetricForDiff(r *carbonzipperpb3.FetchResponse) *carbonpb.Metric {
	m := &carbonpb.Metric{
		Metric: r.Name,
	}
	for i, v := range r.Values {
		if r.IsAbsent[i] {
			v = math.NaN()
		}
		m.Points = append(m.Points, carbonpb.Point{
			Timestamp: uint32(r.StartTime) + uint32(i)*uint32(r.StepTime),
			Value:     v,
		})
	}
	return m
}

func convertFetchResponsesToMetricForMerge(src, dest *carbonzipperpb3.FetchResponse) (*carbonpb.Metric, error) {
	err := ensureSameStartStopStepTime(src, dest)
	if err != nil {
		return nil, err
	}

	m := &carbonpb.Metric{
		Metric: dest.Name,
	}
	for i, v := range src.Values {
		if !dest.IsAbsent[i] || src.IsAbsent[i] {
			continue
		}
		m.Points = append(m.Points, carbonpb.Point{
			Timestamp: uint32(src.StartTime) + uint32(i)*uint32(src.StepTime),
			Value:     v,
		})
	}
	return m, nil
}

func convertFetchResponsesToMetricForOverwrite(src, dest *carbonzipperpb3.FetchResponse) (*carbonpb.Metric, error) {
	err := ensureSameStartStopStepTime(src, dest)
	if err != nil {
		return nil, err
	}

	m := &carbonpb.Metric{
		Metric: dest.Name,
	}
	for i, v := range src.Values {
		if src.IsAbsent[i] || (!dest.IsAbsent[i] && dest.Values[i] == v) {
			continue
		}
		m.Points = append(m.Points, carbonpb.Point{
			Timestamp: uint32(src.StartTime) + uint32(i)*uint32(src.StepTime),
			Value:     v,
		})
	}
	return m, nil
}

func ensureSameStartStopStepTime(src, dest *carbonzipperpb3.FetchResponse) error {
	if src.StartTime != dest.StartTime {
		return fmt.Errorf("StartTime unmatched, src.Name=%s, src.StartTime=%d, dest.Name=%s, dest.StartTime=%d", src.Name, src.StartTime, dest.Name, dest.StartTime)
	}
	if src.StopTime != dest.StopTime {
		return fmt.Errorf("StopTime unmatched, src.Name=%s, src.StopTime=%d, dest.Name=%s, dest.StopTime=%d", src.Name, src.StopTime, dest.Name, dest.StopTime)
	}
	if src.StepTime != dest.StepTime {
		return fmt.Errorf("StepTime unmatched, src.Name=%s, src.StepTime=%d, dest.Name=%s, dest.StepTime=%d", src.Name, src.StepTime, dest.Name, dest.StepTime)
	}
	return nil
}
