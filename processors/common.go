package processors

import "github.com/congqixia/milvus-log-parser/parser"

type Filter interface {
	FiltersEntry(parser.Entry) bool
}

type FilterFunc func(parser.Entry) bool

func (f FilterFunc) FiltersEntry(entry parser.Entry) bool {
	return f(entry)
}

func FiltersMsg(msg string) FilterFunc {
	return func(entry parser.Entry) bool {
		return entry.Msg == msg
	}
}
