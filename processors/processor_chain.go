package processors

import "github.com/congqixia/ranger/parser"

// ProcesserChain chains a list of processor.
type ProcessorChain struct {
	processors []Processor
}

func (pc *ProcessorChain) ProcessEntry(entry *parser.Entry) {
	for _, p := range pc.processors {
		p.ProcessEntry(entry)
	}
}

// ChainProcessor add processor into chain
func (pc *ProcessorChain) ChainProcessor(p Processor) {
	pc.processors = append(pc.processors, p)
}
