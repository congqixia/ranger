package processors

import (
	"strconv"
	"strings"
	"time"

	"github.com/congqixia/milvus-log-parser/parser"
)

const (
	flushStart    = `receive flush request`
	flushSegments = `flush response with segments`
)

// FlushProcessor extracts flush related information in datacoord.
type FlushProcessor struct {
	Info map[int64]*FlushInfo
}

type FlushInfo struct {
	CollectionID int64
	Events       []FlushEvent
}

type EventType int32

const (
	FlushStartEvent = iota + 1
	FlushResponseSegments
)

type FlushEvent struct {
	TS        time.Time
	EventType EventType
	Segments  []int64
}

// ProcessEntry implements Processor.
func (p *FlushProcessor) ProcessEntry(entry parser.Entry) {
	if entry.Err != nil {
		return
	}

	// TODO add init or new function
	if p.Info == nil {
		p.Info = make(map[int64]*FlushInfo)
	}

	switch entry.Msg {
	case flushStart:
		collectionID := p.getCollectionID(entry)
		if collectionID <= 0 {
			return
		}

		info, has := p.Info[collectionID]
		if !has {
			info = &FlushInfo{
				CollectionID: collectionID,
				Events:       nil,
			}
			p.Info[collectionID] = info
		}

		info.Events = append(info.Events, FlushEvent{
			TS:        entry.TS,
			EventType: FlushStartEvent,
			Segments:  nil,
		})
	case flushSegments:
		collectionID := p.getCollectionID(entry)
		if collectionID <= 0 {
			return
		}

		segments := p.getSegments(entry)
		info, has := p.Info[collectionID]

		if !has {
			info = &FlushInfo{
				CollectionID: collectionID,
				Events:       nil,
			}
			p.Info[collectionID] = info
		}

		info.Events = append(info.Events, FlushEvent{
			TS:        entry.TS,
			EventType: FlushResponseSegments,
			Segments:  segments,
		})
	}
}

func (p *FlushProcessor) getCollectionID(entry parser.Entry) int64 {
	for _, kv := range entry.Data {
		if kv.Key == "collectionID" {
			id, _ := strconv.ParseInt(kv.Value, 10, 64)
			return id
		}
	}
	return 0
}

func (p *FlushProcessor) getSegments(entry parser.Entry) []int64 {
	for _, kv := range entry.Data {
		if kv.Key == "segments" {
			if strings.HasPrefix(kv.Value, `"[`) && strings.HasSuffix(kv.Value, `]"`) {
				raw := kv.Value[2 : len(kv.Value)-2]
				if len(raw) > 0 {
					parts := strings.Split(raw, ",")
					result := make([]int64, 0, len(parts))
					for _, part := range parts {
						part = strings.TrimSpace(part)
						id, err := strconv.ParseInt(part, 10, 64)
						if err == nil {
							result = append(result, id)
						}
					}
					return result
				}
			}
		}
	}
	return nil
}
