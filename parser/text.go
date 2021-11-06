package parser

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	ErrInvalidLogFormat = errors.New("non [] formated value found")

	ErrInvalidLogLevel = errors.New("in valid log level")
)

// ZapTextParser parses zap text format log line data.
type ZapTextParser struct {
	err    error
	Ts     time.Time
	level  zapcore.Level
	Caller string
	Msg    string
	Data   []logKV
}

// logKV key value for log extra items.
type logKV struct {
	Key   string
	Value string
}

// ParseLine do the parsing procedure.
func (p *ZapTextParser) ParseLine(line []byte) {
	p.Data = nil

	var buffer []byte

	var cur int

	buffer, cur = p.readNextBlock(line, cur)

	if cur != -1 {
		p.Ts, p.err = time.Parse("2006/01/02 15:04:05.000 -07:00", string(buffer))
	} else {
		p.err = ErrInvalidLogFormat

		return
	}

	buffer, cur = p.readNextBlock(line, cur)
	if cur != -1 {
		switch string(buffer) {
		case "DEBUG":
			p.level = zap.DebugLevel
		case "INFO":
			p.level = zap.InfoLevel
		case "WARN":
			p.level = zap.WarnLevel
		case "ERROR":
			p.level = zap.ErrorLevel
		default:
			p.err = ErrInvalidLogLevel
		}
	}

	buffer, cur = p.readNextBlock(line, cur)
	if cur != -1 {
		p.Caller = string(buffer)
	}

	buffer, cur = p.readNextBlock(line, cur)
	if cur != -1 {
		p.Msg = string(buffer)
	}

	for cur != -1 {
		buffer, cur = p.readNextBlock(line, cur)
		if cur != -1 {
			sep := bytes.IndexByte(buffer, '=')
			if sep == -1 {
				p.Data = append(p.Data, logKV{
					Key:   "",
					Value: string(buffer),
				})
			} else {
				p.Data = append(p.Data, logKV{
					Key:   string(buffer[:sep]),
					Value: string(buffer[sep+1:]),
				})
			}
		}
	}
}

func (p *ZapTextParser) Err() error {
	return p.err
}

func (p *ZapTextParser) String() string {
	return fmt.Sprintf("%v %v %v %v", p.Ts, p.level, p.Caller, p.Msg)
}

func (p *ZapTextParser) readNextBlock(line []byte, idx int) ([]byte, int) {
	if idx < 0 {
		return nil, idx
	}

	for idx < len(line) && line[idx] != '[' {
		idx++
	}

	start := idx + 1

	idx++

	inStr := false

	for idx < len(line) && (line[idx] != ']' || inStr) {
		if line[idx] == '"' {
			inStr = !inStr
		}
		idx++
	}
	// found closing ']'
	if idx < len(line) && line[idx] == ']' {
		return line[start:idx], idx + 1
	}

	return nil, -1
}
