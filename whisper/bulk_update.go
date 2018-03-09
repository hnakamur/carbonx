package whisper

import (
	"fmt"
	"os"
	"path/filepath"

	whisp "github.com/go-graphite/go-whisper"
	"github.com/hnakamur/ltsvlog"
)

type BulkUpdate struct {
	Path    string
	Options *whisp.Options

	Retentions        whisp.Retentions
	AggregationMethod whisp.AggregationMethod
	XFilesFactor      float32
}

func (u *BulkUpdate) Update(points []*whisp.TimeSeriesPoint) error {
	ltsvlog.Logger.Info().String("msg", "BulkUpdate.Update").String("path", u.Path).
		Fmt("retentions", "%+v", u.Retentions).
		Fmt("aggregation", "%+v", u.AggregationMethod).Float32("xFilesFactor", u.XFilesFactor).
		Fmt("options", "%+v", u.Options).Log()

	w, err := whisp.OpenWithOptions(u.Path, u.Options)
	if err != nil {
		if !os.IsNotExist(err) {
			return ltsvlog.WrapErr(err, func(err error) error {
				return fmt.Errorf("failed to open whisper file, err=%v", err)
			}).String("path", u.Path).Fmt("options", "%+v", u.Options).Stack("")
		}

		err = os.MkdirAll(filepath.Dir(u.Path), os.ModeDir|os.ModePerm)
		if err != nil {
			return ltsvlog.WrapErr(err, func(err error) error {
				return fmt.Errorf("failed to mkdir for whisper file, err=%v", err)
			}).String("path", u.Path).Stack("")
		}

		w, err = whisp.CreateWithOptions(u.Path, u.Retentions, u.AggregationMethod, u.XFilesFactor, u.Options)
		if err != nil {
			return ltsvlog.WrapErr(err, func(err error) error {
				return fmt.Errorf("failed to create whisper file, err=%v", err)
			}).String("path", u.Path).Fmt("retentions", "%+v", u.Retentions).
				Fmt("aggregation", "%+v", u.AggregationMethod).Float32("xFilesFactor", u.XFilesFactor).
				Fmt("options", "%+v", u.Options).Stack("")
		}
	}
	defer w.Close()

	err = w.UpdateMany(points)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to update whisper file points, err=%v", err)
		}).String("path", u.Path).Fmt("points", "%+v", points).Stack("")
	}
	return nil
}
