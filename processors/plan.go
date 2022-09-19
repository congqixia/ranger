package processors

// ProcessPlan is the plan to define how to process the log.
type ProcessPlan struct {
	Items []LogItem
}

// LogItem defines the log item.
type LogItem struct {
	// Short is the item identifier used for reading.
	Short string
	// Tag is the log string start with.
	Tag string
	// Attributes defines the information from log tags.
	Attributes []LogAttr
}

type AttrType int

const (
	AttrTypeInt64 = iota + 1
	AttrTypeString
)

type LogAttr struct {
	Tag  string
	Type AttrType
}
