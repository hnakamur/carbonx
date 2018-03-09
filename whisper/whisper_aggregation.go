package whisper

/*
Schemas read code from https://github.com/grobian/carbonwriter/
*/

import (
	"fmt"
	"regexp"
	"strconv"

	whisper "github.com/go-graphite/go-whisper"
)

type whisperAggregationItem struct {
	name                 string
	pattern              *regexp.Regexp
	xFilesFactor         float64
	aggregationMethodStr string
	aggregationMethod    whisper.AggregationMethod
}

// whisperAggregation ...
type whisperAggregation struct {
	Data    []*whisperAggregationItem
	Default *whisperAggregationItem
}

// newWhisperAggregation create instance of whisperAggregation
func newWhisperAggregation() *whisperAggregation {
	return &whisperAggregation{
		Data: make([]*whisperAggregationItem, 0),
		Default: &whisperAggregationItem{
			name:                 "default",
			pattern:              nil,
			xFilesFactor:         0.5,
			aggregationMethodStr: "average",
			aggregationMethod:    whisper.Average,
		},
	}
}

// readWhisperAggregation ...
func readWhisperAggregation(filename string) (*whisperAggregation, error) {
	config, err := parseIniFile(filename)
	if err != nil {
		return nil, err
	}

	result := newWhisperAggregation()

	for _, section := range config {
		item := &whisperAggregationItem{}
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		item.name = section["name"]

		item.pattern, err = regexp.Compile(section["pattern"])
		if err != nil {
			return nil, fmt.Errorf("failed to parse pattern %#v for [%s]: %s",
				section["pattern"], item.name, err.Error())
		}

		item.xFilesFactor, err = strconv.ParseFloat(section["xfilesfactor"], 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse xFilesFactor %#v in %s: %s",
				section["xfilesfactor"], item.name, err.Error())
		}

		item.aggregationMethodStr = section["aggregationmethod"]
		switch item.aggregationMethodStr {
		case "average", "avg":
			item.aggregationMethod = whisper.Average
		case "sum":
			item.aggregationMethod = whisper.Sum
		case "last":
			item.aggregationMethod = whisper.Last
		case "max":
			item.aggregationMethod = whisper.Max
		case "min":
			item.aggregationMethod = whisper.Min
		default:
			return nil, fmt.Errorf("unknown aggregation method '%s'",
				section["aggregationmethod"])
		}

		result.Data = append(result.Data, item)
	}

	return result, nil
}

// Match find schema for metric
func (a *whisperAggregation) match(metric string) *whisperAggregationItem {
	for _, s := range a.Data {
		if s.pattern.MatchString(metric) {
			return s
		}
	}
	return a.Default
}
