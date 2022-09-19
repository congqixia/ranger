package processors

import "github.com/congqixia/ranger/parser"

// Processor processes entries from parser and do logics.
type Processor interface {
	ProcessEntry(*parser.Entry)
}
