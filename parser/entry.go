package parser

import (
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
}

// LogKV key value for log extra items.
type LogKV struct {
	Key   string
	Value string
}
