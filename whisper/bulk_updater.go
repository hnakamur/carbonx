package whisper

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	whisp "github.com/go-graphite/go-whisper"
	"github.com/hnakamur/carbonx/carbonpb"
	"github.com/hnakamur/ltsvlog"
)

// BulkUpdater update many points for one metric at once.
type BulkUpdater struct {
	rootPath string
	options  *Options

	schemas     *whisperSchemas
	aggregation *whisperAggregation
}

// Options is used when creating a whisper file.
type Options = whisp.Options

// TimeSeriesPoint is points to update.
type TimeSeriesPoint = whisp.TimeSeriesPoint

// NewBulkUpdater creates a new BulkUpdater.
func NewBulkUpdater(rootPath, schemasPath, aggregationPath string, options *Options) (*BulkUpdater, error) {
	schemas, err := readWhisperSchemas(schemasPath)
	if err != nil {
		return nil, ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read whisper schemas file, err=%v", err)
		}).String("schemasPath", schemasPath).Stack("")
	}

	agg, err := readWhisperAggregation(aggregationPath)
	if err != nil {
		return nil, ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read whisper aggregation file, err=%v", err)
		}).String("aggregationPath", aggregationPath).Stack("")
	}

	return &BulkUpdater{
		rootPath:    rootPath,
		options:     options,
		schemas:     &schemas,
		aggregation: agg,
	}, nil
}

// Update updates many points for one metric at once.
// If the whisper file for the metric does not exist, it will be created first.
func (u *BulkUpdater) Update(metric string, points []*TimeSeriesPoint) error {
	path := MetricFilePath(u.rootPath, metric)
	w, err := whisp.OpenWithOptions(path, u.options)
	if err != nil {
		if !os.IsNotExist(err) {
			return ltsvlog.WrapErr(err, func(err error) error {
				return fmt.Errorf("failed to open whisper file, err=%v", err)
			}).String("path", path).Fmt("options", "%+v", u.options).Stack("")
		}

		schema, ok := u.schemas.Match(metric)
		if !ok {
			return ltsvlog.Err(fmt.Errorf("no storage schema defined for metric %s", metric)).
				String("metric", metric).Stack("")
		}

		aggr := u.aggregation.match(metric)
		if aggr == nil {
			return ltsvlog.Err(fmt.Errorf("no storage schema defined for metric %s", metric)).
				String("metric", metric).Stack("")
		}

		err = os.MkdirAll(filepath.Dir(path), os.ModeDir|os.ModePerm)
		if err != nil {
			return ltsvlog.WrapErr(err, func(err error) error {
				return fmt.Errorf("failed to mkdir for whisper file, err=%v", err)
			}).String("path", path).Stack("")
		}

		w, err = whisp.CreateWithOptions(path, schema.Retentions, aggr.aggregationMethod,
			float32(aggr.xFilesFactor), u.options)
		if err != nil {
			return ltsvlog.WrapErr(err, func(err error) error {
				return fmt.Errorf("failed to create whisper file, err=%v", err)
			}).String("path", path).Fmt("retentions", "%+v", schema.Retentions).
				Fmt("aggregation", "%+v", aggr.aggregationMethod).Float32("xFilesFactor", float32(aggr.xFilesFactor)).
				Fmt("options", "%+v", u.options).Stack("")
		}
	}
	defer w.Close()

	err = w.UpdateMany(points)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to update whisper file points, err=%v", err)
		}).String("path", path).Fmt("points", "%+v", points).Stack("")
	}
	return nil
}

func MetricFilePath(rootPath, metric string) string {
	return filepath.Join(rootPath, filepath.Join(strings.Split(metric, ".")...)) + ".wsp"
}

func TimeSeriesPointsFromMetric(metric *carbonpb.Metric) (name string, points []*TimeSeriesPoint) {
	points = make([]*TimeSeriesPoint, len(metric.Points))
	for i, p := range metric.Points {
		points[i] = &TimeSeriesPoint{
			Time:  int(p.Timestamp),
			Value: p.Value,
		}
	}
	return metric.Metric, points
}
