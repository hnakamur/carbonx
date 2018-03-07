package carbonx

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/carbonx/carbonzipperpb3"
	"github.com/hnakamur/carbonx/sender"
	"github.com/hnakamur/ltsvlog"
	retry "github.com/rafaeljesus/retry-go"
)

var ErrUnmatchedInfo = errors.New("unmatched info")

type Merger struct {
	srcClient  *Client
	destClient *Client
	destSender *sender.TCPSender
}

func NewMerger(src, dest *Client, sender *sender.TCPSender) *Merger {
	return &Merger{
		srcClient:  src,
		destClient: dest,
		destSender: sender,
	}
}

func (m *Merger) Diff(metric string, from, until time.Time) ([][]carbonpb.Point, error) {
	srcData, err := m.srcClient.FetchData(metric, from, until)
	if err != nil {
		return nil, err
	}
	destData, err := m.destClient.FetchData(metric, from, until)
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

func (m *Merger) MergeMetrics(metrics []string) error {
	for _, metric := range metrics {
		err := m.MergeMetric(metric)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Merger) MergeMetric(metric string) error {
	const attempts = 5
	const sleepTime = 100 * time.Millisecond
	var srcInfo, destInfo *carbonzipperpb3.InfoResponse
	var srcErr, destErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcErr = retry.Do(func() error {
			var err error
			srcInfo, err = m.srcClient.GetMetricInfo(metric)
			return err
		}, attempts, sleepTime)
	}()
	go func() {
		defer wg.Done()
		destErr = retry.Do(func() error {
			var err error
			destInfo, err = m.destClient.GetMetricInfo(metric)
			return err
		}, attempts, sleepTime)
	}()
	wg.Wait()

	if srcErr == ErrNotFound {
		ltsvlog.Logger.Info().String("msg", "skip metric which does not exist in src").String("metric", metric).Log()
		return nil
	} else if srcErr != nil {
		return ltsvlog.WrapErr(srcErr, func(err error) error {
			return fmt.Errorf("failed to get src info for metric=%s, err=%v", metric, srcErr)
		}).String("metric", metric).Stack("")
	}

	if destErr != nil && destErr == ErrNotFound {
		return ltsvlog.WrapErr(destErr, func(err error) error {
			return fmt.Errorf("failed to get dest info for metric=%s, err=%v", metric, destErr)
		}).String("metric", metric).Stack("")
	}

	if destInfo != nil && !sameInfo(*srcInfo, *destInfo) {
		return ltsvlog.Err(ErrUnmatchedInfo).
			String("metric", metric).Fmt("srcInfo", "%+v", srcInfo).Fmt("destInfo", "%+v", destInfo).Stack("")
	}

	//ltsvlog.Logger.Info().String("msg", "infos").Sprintf("srcInfo", "%+v", srcInfo).Sprintf("destInfo", "%+v", destInfo).Log()
	retentions := srcInfo.Retentions
	for i := len(retentions) - 1; i >= 0; i-- {
		r := retentions[i]
		fromOffset := -time.Duration(r.SecondsPerPoint) * time.Duration(r.NumberOfPoints) * time.Second
		var untilOffset time.Duration
		if i > 0 {
			r2 := retentions[i-1]
			untilOffset = -time.Duration(r2.SecondsPerPoint) * time.Duration(r2.NumberOfPoints) * time.Second
		}
		log.Printf("fromOffset=%s, untilOffset=%s", fromOffset, untilOffset)

		step := time.Duration(r.SecondsPerPoint) * time.Second
		now := time.Now().Truncate(step)
		from := now.Add(fromOffset).Add(step)
		until := now.Add(untilOffset)
		points, err := m.pointsForMerge(metric, from, until)
		if err == ErrNotFound {
			ltsvlog.Logger.Info().String("msg", "skip metric whose src data does not exist").String("metric", metric).Log()
			continue
		} else if err != nil {
			return err
		}

		destMetrics := []*carbonpb.Metric{
			{
				Metric: metric,
				Points: points,
			},
		}
		err = m.destSender.Send(destMetrics)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Merger) pointsForMerge(metric string, from, until time.Time) ([]carbonpb.Point, error) {
	log.Printf("pointsForMerge start, from=%s (%d), until=%s (%d)", from, from.Unix(), until, until.Unix())
	var srcData, destData *carbonzipperpb3.FetchResponse
	var srcErr, destErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcData, srcErr = m.srcClient.FetchData(metric, from, until)
		srcPoints := convertFetchedDataToPoints(srcData)
		log.Printf("from=%s (%d), until=%s (%d), srcPoints=%+v", from, from.Unix(), until, until.Unix(), srcPoints)
	}()
	go func() {
		defer wg.Done()
		destData, destErr = m.destClient.FetchData(metric, from, until)
	}()
	wg.Wait()

	if srcErr != nil {
		return nil, srcErr
	}

	srcPoints := convertFetchedDataToPoints(srcData)
	destPoints := convertFetchedDataToPoints(destData)
	log.Printf("from=%s (%d), until=%s (%d), srcPoints=%+v, destPoints=%+v", from, from.Unix(), until, until.Unix(), srcPoints, destPoints)

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

	var points []carbonpb.Point
	for i := range srcData.Values {
		if srcData.IsAbsent[i] || destData == nil || !destData.IsAbsent[i] {
			continue
		}

		ts := uint32(srcData.StartTime) + uint32(i)*uint32(srcData.StepTime)
		srcValue := srcData.Values[i]
		points = append(points, carbonpb.Point{
			Timestamp: ts,
			Value:     srcValue,
		})
	}
	return points, nil
}
