package parser

import (
	"bytes"
	"errors"
	"strings"
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
	line []byte
	cur  int
}

// ParseLine do the parsing procedure.
func (p *ZapTextParser) ParseLine(line []byte) Entry {
	entry := Entry{}
	p.line = line
	p.cur = 0

	entry.TS, entry.Err = p.parseTimeStamp()
	if entry.Err != nil {
		return entry
	}

	entry.Level, entry.Err = p.parseLogLevel()
	if entry.Err != nil {
		return entry
	}

	entry.Caller, entry.Err = p.parseCaller()
	if entry.Err != nil {
		return entry
	}

	entry.Msg, entry.Err = p.parseMsg()
	if entry.Err != nil {
		return entry
	}

	entry.Data = p.parseData()

	return entry
}

func (p *ZapTextParser) parseTimeStamp() (time.Time, error) {
	var buffer []byte
	buffer, p.cur = p.readNextBlock(p.line, p.cur)

	if p.cur != -1 {
		return time.Parse("2006/01/02 15:04:05.000 -07:00", string(buffer))
	} else {
		return time.Time{}, ErrInvalidLogFormat
	}
}

func (p *ZapTextParser) parseLogLevel() (zapcore.Level, error) {
	var buffer []byte
	buffer, p.cur = p.readNextBlock(p.line, p.cur)
	if p.cur != -1 {
		switch string(buffer) {
		case "DEBUG":
			return zap.DebugLevel, nil
		case "INFO":
			return zap.InfoLevel, nil
		case "WARN":
			return zap.WarnLevel, nil
		case "ERROR":
			return zap.ErrorLevel, nil
		default:
			return 0, ErrInvalidLogLevel
		}
	}
	return 0, ErrInvalidLogLevel
}

func (p *ZapTextParser) parseCaller() (string, error) {
	var buffer []byte
	buffer, p.cur = p.readNextBlock(p.line, p.cur)
	if p.cur != -1 {
		return string(buffer), nil
	}
	return "", ErrInvalidLogFormat
}

func (p *ZapTextParser) parseMsg() (string, error) {
	var buffer []byte
	buffer, p.cur = p.readNextBlock(p.line, p.cur)
	if p.cur != -1 {
		if len(buffer) > 0 && buffer[0] == '"' && buffer[len(buffer)-1] == '"' {
			return string(buffer[1 : len(buffer)-1]), nil
		} else {
			return string(buffer), nil
		}
	}
	return "", ErrInvalidLogFormat
}

func (p *ZapTextParser) parseData() []LogKV {
	var buffer []byte
	var result []LogKV

	for p.cur != -1 {
		buffer, p.cur = p.readNextBlock(p.line, p.cur)
		if p.cur != -1 {
			sep := bytes.IndexByte(buffer, '=')
			if sep == -1 {
				result = append(result, LogKV{
					Key:   "",
					Value: string(buffer),
				})
			} else {
				key := string(buffer[:sep])
				if strings.HasPrefix(key, `"`) && strings.HasSuffix(key, `"`) {
					key = key[1 : len(key)-1]
				}
				result = append(result, LogKV{
					Key:   key,
					Value: string(buffer[sep+1:]),
				})
			}
		}
	}
	return result
}

func (p *ZapTextParser) Err() error {
	return nil
}

func (p *ZapTextParser) String() string {
	//	return fmt.Sprintf("%v %v %v %v", p.Ts, p.level, p.Caller, p.Msg)
	return ""
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
