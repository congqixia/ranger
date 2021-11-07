package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZapParser_parseLine(t *testing.T) {
	p := &ZapTextParser{}
	entry := p.ParseLine([]byte(`[2021/11/03 14:46:24.406 +08:00] [DEBUG] [session_util.go:185] ["Session Register Begin"]`))
	if entry.Err != nil {
		t.Log(entry.Err.Error())
	}
	assert.Nil(t, entry.Err)
	t.Log(entry.TS)
	val := `2021-11-02T01:08:34.769194163Z stdout F [2021/11/02 01:08:34.769 +00:00] [INFO] [grpclog.go:37] ["[transport]transport: loopyWriter.run returning. connection error: desc = \"transport is closing\""]`
	p.ParseLine([]byte(val))
	if entry.Err != nil {
		t.Log(entry.Err.Error())
	}
	assert.Nil(t, p.Err)
	t.Log(entry.TS)
}
