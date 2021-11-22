package processors

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/congqixia/milvus-log-parser/parser"
)

const (
	flushStart           = `receive flush request`
	flushSegments        = `flush response with segments`
	saveBiglogPathsStart = `receive SaveBinlogPaths request`

	getRecoveryStart         = `receive get recovery info request`
	getRecoveryAppendChannel = `datacoord append channelInfo in GetRecoveryInfo`
)

type GetRecoveryInfoProcessor struct {
	Records []Record
}

func (p *GetRecoveryInfoProcessor) ProcessEntry(entry *parser.Entry) {
	switch entry.Msg {
	case getRecoveryStart:
		collectionID, _ := entry.SearchDataInt64("collectionID")
		partitionID, _ := entry.SearchDataInt64("partitionID")
		p.Records = append(p.Records, Record{
			RecordType:   "GetRecoveryInfo",
			Point:        RecordStart,
			Ts:           entry.TS,
			CollectionID: collectionID,
			PartitionID:  partitionID,
		})
	case getRecoveryAppendChannel:
		collectionID, _ := entry.SearchDataInt64("collectionID")
		chanInfo, _ := entry.SearchData("channelInfo")
		p.Records = append(p.Records, Record{
			RecordType:   "GetRecoveryInfo",
			Point:        RecordOther,
			Ts:           entry.TS,
			CollectionID: collectionID,
			Extra: []parser.LogKV{
				{
					Key:   "ChannelInfo",
					Value: chanInfo,
				},
			},
		})
	default:
		return
	}
	entry.Processed = true
}

type Record struct {
	RecordType   string
	Point        RecordPoint
	Ts           time.Time
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	Extra        []parser.LogKV
}

type RecordPoint int32

const (
	RecordStart = iota + 1
	RecordSuccess
	RecordFail
	RecordOther
)

// FlushProcessor extracts flush related information in datacoord.
type FlushProcessor struct {
	Info map[int64]*FlushInfo
}

type FlushInfo struct {
	CollectionID int64
	Events       []Event
}

type EventType int32

const (
	FlushStartEvent = iota + 1
	FlushResponseSegments
	SaveBinlogPathsRequestRecv
)

type Event interface {
	EventTS() time.Time
	EvtType() EventType

	Display() string
}

type FlushEvent struct {
	TS        time.Time
	EventType EventType
}

func (fe FlushEvent) EventTS() time.Time {
	return fe.TS
}

func (fe FlushEvent) EvtType() EventType {
	return fe.EventType
}

func (fe FlushEvent) Display() string {
	return fmt.Sprintf("%d ts: %v", fe.EventType, fe.TS)
}

type FlushWithSegment struct {
	FlushEvent

	Segments []int64
}

func (e FlushWithSegment) Display() string {
	return fmt.Sprintf("%d ts: %v, segments: %v", e.EventType, e.TS, e.Segments)
}

type SaveBinlogPaths struct {
	FlushEvent

	Segment   int64
	IsFlush   bool
	NumOfRows int64
}

// Display prints info about SaveBinlogPaths
func (e SaveBinlogPaths) Display() string {
	return fmt.Sprintf("%d ts: %v segment: %d, flushed:%v, num rows: %d",
		e.EventType, e.TS, e.Segment, e.IsFlush, e.NumOfRows)
}

type Checkpoint struct {
	SegmentID int64 `json:"segmentID"`
	NumOfRows int64 `json:"num_of_rows"`
}

// ProcessEntry implements Processor.
func (p *FlushProcessor) ProcessEntry(entry *parser.Entry) {
	if entry.Err != nil {
		return
	}

	// TODO add init or new function.
	if p.Info == nil {
		p.Info = make(map[int64]*FlushInfo)
	}

	switch entry.Msg {
	case flushStart:
		collectionID := p.getCollectionID(entry)
		if collectionID <= 0 {
			return
		}

		info := p.getFlushInfo(collectionID)

		info.Events = append(info.Events, FlushEvent{
			TS:        entry.TS,
			EventType: FlushStartEvent,
		})
	case flushSegments:
		collectionID := p.getCollectionID(entry)
		if collectionID <= 0 {
			return
		}

		info := p.getFlushInfo(collectionID)
		segments := p.getSegments(entry)

		info.Events = append(info.Events, FlushWithSegment{
			FlushEvent: FlushEvent{
				TS:        entry.TS,
				EventType: FlushResponseSegments,
			},
			Segments: segments,
		})
	case saveBiglogPathsStart:
		collectionID := p.getCollectionID(entry)
		if collectionID <= 0 {
			return
		}

		segmentID, _ := entry.SearchDataInt64("segmentID")
		flushed, _ := entry.SearchDataBool("isFlush")

		cpStr, has := entry.SearchData("checkpoints")
		if has && len(cpStr) > 2 {
			cpStr = cpStr[1 : len(cpStr)-1]
		}
		cpStr = strings.Replace(cpStr, "\\", "", -1)

		numOfRows := int64(0)
		var checkpoints []Checkpoint
		err := json.Unmarshal([]byte(cpStr), &checkpoints)
		if err != nil {
			fmt.Println(cpStr, err.Error())
		}
		for _, cp := range checkpoints {
			if cp.SegmentID == segmentID {
				numOfRows = cp.NumOfRows
			}
		}

		info := p.getFlushInfo(collectionID)
		info.Events = append(info.Events, SaveBinlogPaths{
			FlushEvent: FlushEvent{
				TS:        entry.TS,
				EventType: SaveBinlogPathsRequestRecv,
			},

			Segment:   segmentID,
			IsFlush:   flushed,
			NumOfRows: numOfRows,
		})
	default:
		return
	}
	entry.Processed = true
}

func (p *FlushProcessor) getFlushInfo(collectionID int64) *FlushInfo {
	info, has := p.Info[collectionID]

	if !has {
		info = &FlushInfo{
			CollectionID: collectionID,
			Events:       nil,
		}
		p.Info[collectionID] = info
	}

	return info
}

func (p *FlushProcessor) getCollectionID(entry *parser.Entry) int64 {
	id, has := entry.SearchDataInt64("collectionID")
	if !has {
		return 0
	}

	return id
}

func (p *FlushProcessor) getSegments(entry *parser.Entry) []int64 {
	str, has := entry.SearchData("segments")
	if !has {
		return nil
	}

	if strings.HasPrefix(str, `"[`) && strings.HasSuffix(str, `]"`) {
		str = str[2 : len(str)-2]
	}

	if len(str) > 0 {
		parts := strings.Split(str, ",")
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

	return nil
}
