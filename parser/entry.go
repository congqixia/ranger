package parser

import (
	"strconv"
	"time"

	"go.uber.org/zap/zapcore"
)

// Entry contains log data from parsers.
type Entry struct {
	Err    error
	TS     time.Time
	Level  zapcore.Level
	Caller string
	Msg    string
	Data   []LogKV

	Processed bool // flag about this entry is processed by some processor
}

// LogKV key value for log extra items.
type LogKV struct {
	Key   string
	Value string
}

// SearchData returns the corresponding Value in Data if key matches.
func (e Entry) SearchData(key string) (string, bool) {
	for _, kv := range e.Data {
		if kv.Key == key {
			return kv.Value, true
		}
	}

	return "", false
}

// SearchDataInt64 returns corresponding parsed Value in Data as int64.
func (e Entry) SearchDataInt64(key string) (int64, bool) {
	str, has := e.SearchData(key)
	if !has {
		return 0, false
	}

	val, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, false
	}

	return val, true
}

func (e Entry) SearchDataBool(key string) (bool, bool) {
	str, has := e.SearchData(key)
	if !has {
		return false, false
	}

	switch str {
	case "true":
		return true, true
	case "false":
		return false, true
	default:
		return false, false
	}
}
