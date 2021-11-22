package processors

import (
	"time"

	"github.com/congqixia/milvus-log-parser/parser"
)

// rootcoord msg consts.
const (
	createCollectionEnqueue = "CreateCollection enqueue"
	dropCollectionEnqueue   = "DropCollection enqueue"
	collectionIDName        = "collection name -> id"
)

// CollectionLifeCircleProcessor extracts collection create / drop events in rootcoord.
type CollectionLifeCircleProcessor struct {
	Info    map[string]*CollectionLifeTime
	Name2ID map[string]int64
	ID2Name map[int64]string
}

type CollectionLifeTime struct {
	CreatedAt, DropedAt time.Time
}

// ProcessEntry implements Processor.
func (p *CollectionLifeCircleProcessor) ProcessEntry(entry *parser.Entry) {
	if entry.Err != nil {
		return
	}

	if p.Info == nil {
		p.Info = make(map[string]*CollectionLifeTime)
	}
	if p.ID2Name == nil {
		p.ID2Name = make(map[int64]string)
	}
	if p.Name2ID == nil {
		p.Name2ID = make(map[string]int64)
	}

	switch entry.Msg {
	case createCollectionEnqueue:
		for _, kv := range entry.Data {
			if kv.Key == "collection" {
				lifetime := p.Info[kv.Value]
				if lifetime == nil {
					lifetime = &CollectionLifeTime{}
					p.Info[kv.Value] = lifetime
				}

				lifetime.CreatedAt = entry.TS

				break
			}
		}
	case dropCollectionEnqueue:
		for _, kv := range entry.Data {
			if kv.Key == "collection" {
				lifetime := p.Info[kv.Value]
				if lifetime == nil {
					lifetime = &CollectionLifeTime{}
					p.Info[kv.Value] = lifetime
				}

				lifetime.DropedAt = entry.TS

				break
			}
		}
	case collectionIDName:
		p.parseCollIDName(entry)
	default:
		return
	}
	entry.Processed = true
}

func (p *CollectionLifeCircleProcessor) parseCollIDName(entry *parser.Entry) {
	name, has := entry.SearchData("collection name")
	if !has {
		return
	}

	id, has := entry.SearchDataInt64("collection_id")
	if !has {
		return
	}

	p.ID2Name[id] = name
	p.Name2ID[name] = id
}
