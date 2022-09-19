package processors

import (
	"time"

	"github.com/congqixia/ranger/parser"
)

// ProxyFunctionTimer extracts proxy function timer ts
type ProxyFunctionTimer struct {
	Method      string
	MsgReceived map[string]time.Time
	MsgDone     map[string]time.Time
}

// ProcessEntry implements Processor.
func (p *ProxyFunctionTimer) ProcessEntry(entry *parser.Entry) {
	if entry.Err != nil {
		return
	}
	if p.MsgReceived == nil {
		p.MsgReceived = make(map[string]time.Time)
	}
	if p.MsgDone == nil {
		p.MsgDone = make(map[string]time.Time)
	}

	switch entry.Msg {
	case p.Method + " received":
		traceID, has := entry.SearchData("traceID")
		if !has {
			break
		}
		p.MsgReceived[traceID] = entry.TS
	case p.Method + " done":
		traceID, has := entry.SearchData("traceID")
		if !has {
			break
		}
		p.MsgDone[traceID] = entry.TS
	default:
		return
	}
	entry.Processed = true
}
