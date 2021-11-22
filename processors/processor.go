package processors

import "github.com/congqixia/milvus-log-parser/parser"

// Processor processes entries from parser and do logics.
type Processor interface {
	ProcessEntry(*parser.Entry)
}
