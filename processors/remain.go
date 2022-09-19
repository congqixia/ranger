package processors

import "github.com/congqixia/ranger/parser"

type RemainCounter struct {
	Counter map[string]int64
}

func (p *RemainCounter) ProcessEntry(entry *parser.Entry) {
	if entry.Processed {
		return
	}

	if p.Counter == nil {
		p.Counter = make(map[string]int64)
	}

	p.Counter[entry.Msg]++
}
