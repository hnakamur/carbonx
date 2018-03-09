package whisper

// this is a parser for graphite's storage-schemas.conf
// it supports old and new retention format
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
// based on https://github.com/grobian/carbonwriter but with some improvements

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/go-graphite/go-whisper"
)

// schema represents one schema setting
type schema struct {
	Name         string
	Pattern      *regexp.Regexp
	RetentionStr string
	Retentions   whisper.Retentions
	Priority     int64
}

// whisperSchemas contains schema settings
type whisperSchemas []schema

func (s whisperSchemas) Len() int           { return len(s) }
func (s whisperSchemas) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s whisperSchemas) Less(i, j int) bool { return s[i].Priority >= s[j].Priority }

// Match finds the schema for metric or returns false if none found
func (s whisperSchemas) Match(metric string) (schema, bool) {
	for _, schema := range s {
		if schema.Pattern.MatchString(metric) {
			return schema, true
		}
	}
	return schema{}, false
}

// parseRetentionDefs parses retention definitions into a Retentions structure
func parseRetentionDefs(retentionDefs string) (whisper.Retentions, error) {
	retentions := make(whisper.Retentions, 0)
	for _, retentionDef := range strings.Split(retentionDefs, ",") {
		retentionDef = strings.TrimSpace(retentionDef)
		parts := strings.Split(retentionDef, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("bad retentions spec %q", retentionDef)
		}

		// try old format
		val1, err1 := strconv.ParseInt(parts[0], 10, 0)
		val2, err2 := strconv.ParseInt(parts[1], 10, 0)

		if err1 == nil && err2 == nil {
			retention := whisper.NewRetention(int(val1), int(val2))
			retentions = append(retentions, &retention)
			continue
		}

		// try new format
		retention, err := whisper.ParseRetentionDef(retentionDef)
		if err != nil {
			return nil, err
		}
		retentions = append(retentions, retention)
	}
	return retentions, nil
}

// readWhisperSchemas reads and parses a storage-schemas.conf file and returns a sorted
// schemas structure
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
func readWhisperSchemas(filename string) (whisperSchemas, error) {
	config, err := parseIniFile(filename)
	if err != nil {
		return nil, err
	}

	var schemas whisperSchemas

	for i, section := range config {
		schema := schema{}
		schema.Name = section["name"]

		if section["pattern"] == "" {
			return nil, fmt.Errorf("[persister] Empty pattern for [%s]", schema.Name)
		}
		schema.Pattern, err = regexp.Compile(section["pattern"])
		if err != nil {
			return nil, fmt.Errorf("[persister] Failed to parse pattern %q for [%s]: %s",
				section["pattern"], schema.Name, err.Error())
		}
		schema.RetentionStr = section["retentions"]
		schema.Retentions, err = parseRetentionDefs(schema.RetentionStr)

		if err != nil {
			return nil, fmt.Errorf("[persister] Failed to parse retentions %q for [%s]: %s",
				schema.RetentionStr, schema.Name, err.Error())
		}

		p := int64(0)
		if section["priority"] != "" {
			p, err = strconv.ParseInt(section["priority"], 10, 0)
			if err != nil {
				return nil, fmt.Errorf("[persister] Failed to parse priority %q for [%s]: %s", section["priority"], schema.Name, err)
			}
		}
		schema.Priority = int64(p)<<32 - int64(i) // to sort records with same priority by position in file

		schemas = append(schemas, schema)
	}

	sort.Sort(schemas)
	return schemas, nil
}
