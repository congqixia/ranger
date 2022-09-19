package processors

import "github.com/congqixia/ranger/parser"

// PlanParseResult storges plan parse result
type PlanParseResult struct {
	Short   string
	Tag     string
	Int64s  []AttrResult[int64]
	Strings []AttrResult[string]
}

// PlanProcessor is the process executes according to the plan
type PlanProcessor struct {
	Plan ProcessPlan

	Results []PlanParseResult
}

func (p *PlanProcessor) ProcessEntry(entry *parser.Entry) {
	if entry.Err != nil {
		return
	}

	for _, item := range p.Plan.Items {
		// tag matches
		if entry.Msg == item.Tag {
			result := p.extraAttributes(entry, item)
			p.Results = append(p.Results, result)
		}
	}
}

func (p *PlanProcessor) extraAttributes(entry *parser.Entry, item LogItem) PlanParseResult {
	result := PlanParseResult{Tag: item.Tag, Short: item.Short}
	for _, attr := range item.Attributes {
		switch attr.Type {
		case AttrTypeInt64:
			id, has := entry.SearchDataInt64(attr.Tag)
			if has {
				result.Int64s = append(result.Int64s, AttrResult[int64]{Tag: attr.Tag, Value: id})
			}
		case AttrTypeString:
			str, has := entry.SearchData(attr.Tag)
			if has {
				result.Strings = append(result.Strings, AttrResult[string]{Tag: attr.Tag, Value: str})
			}
		default:
		}
	}
	return result
}

// AttrResult attribute parse result
type AttrResult[e any] struct {
	Tag   string
	Value e
}
