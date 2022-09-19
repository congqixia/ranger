package processors

import "github.com/congqixia/ranger/parser"

var (
	TimeFormat = `2006-01-02 15:04:05.000 -0700`
)

// Filter defines utility filters entries
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
