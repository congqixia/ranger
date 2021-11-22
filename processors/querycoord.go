package processors

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/congqixia/milvus-log-parser/parser"
)

const (
	loadAssignSegmentSuccess = "assignInternalTask: assign segment to node success"
)

type LoadCollectionProcessor struct {
	Records []Record
}

func (p *LoadCollectionProcessor) ProcessEntry(entry *parser.Entry) {
	switch entry.Msg {
	case loadAssignSegmentSuccess:
		val, _ := entry.SearchData("load segments requests")
		val = val[1 : len(val)-1]
		val = strings.Replace(val, "\\", "", -1)
		var reqs []LoadSegmentsRequest
		err := json.Unmarshal([]byte(val), &reqs)
		if err == nil {
			for _, req := range reqs {
				record := Record{
					Ts:           entry.TS,
					RecordType:   "Load Segments",
					Point:        RecordStart,
					CollectionID: req.CollectionID,
				}
				record.Extra = append(record.Extra, parser.LogKV{
					Key:   "QueryNode ID",
					Value: fmt.Sprintf("%d", req.DstNodeID),
				})
				var segments []int64
				for _, info := range req.Infos {
					segments = append(segments, info.SegmentID)
				}
				record.Extra = append(record.Extra, parser.LogKV{
					Key:   "segments",
					Value: fmt.Sprintf("%v", segments),
				})
				p.Records = append(p.Records, record)
			}
			/*
				for _, req := range reqs {
					fmt.Println("collection", req.CollectionID)
					for _, info := range req.Infos {:q
						fmt.Println("segment", info.SegmentID)
					}

				}*/
		} else {
			fmt.Println(err.Error())
		}
	default:
		return
	}

	entry.Processed = true
}

type LoadSegmentsRequest struct {
	DstNodeID    int64              `json:"dst_nodeID,omitempty"`
	Infos        []*SegmentLoadInfo `protobuf:"bytes,3,rep,name=infos,proto3" json:"infos,omitempty"`
	SourceNodeID int64              `"json:"source_nodeID,omitempty"`
	CollectionID int64              `"json:"collectionID,omitempty"`
}

//used for handoff task
type SegmentLoadInfo struct {
	SegmentID      int64           `protobuf:"varint,1,opt,name=segmentID,proto3" json:"segmentID,omitempty"`
	PartitionID    int64           `protobuf:"varint,2,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	CollectionID   int64           `protobuf:"varint,3,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	DbID           int64           `protobuf:"varint,4,opt,name=dbID,proto3" json:"dbID,omitempty"`
	FlushTime      int64           `protobuf:"varint,5,opt,name=flush_time,json=flushTime,proto3" json:"flush_time,omitempty"`
	BinlogPaths    []*FieldBinlog  `protobuf:"bytes,6,rep,name=binlog_paths,json=binlogPaths,proto3" json:"binlog_paths,omitempty"`
	NumOfRows      int64           `protobuf:"varint,7,opt,name=num_of_rows,json=numOfRows,proto3" json:"num_of_rows,omitempty"`
	Statslogs      []*FieldBinlog  `protobuf:"bytes,8,rep,name=statslogs,proto3" json:"statslogs,omitempty"`
	Deltalogs      []*DeltaLogInfo `protobuf:"bytes,9,rep,name=deltalogs,proto3" json:"deltalogs,omitempty"`
	CompactionFrom []int64         `protobuf:"varint,10,rep,packed,name=compactionFrom,proto3" json:"compactionFrom,omitempty"`
	EnableIndex    bool            `protobuf:"varint,11,opt,name=enable_index,json=enableIndex,proto3" json:"enable_index,omitempty"`
}

type FieldBinlog struct {
	FieldID int64    `protobuf:"varint,1,opt,name=fieldID,proto3" json:"fieldID,omitempty"`
	Binlogs []string `protobuf:"bytes,2,rep,name=binlogs,proto3" json:"binlogs,omitempty"`
}

type DeltaLogInfo struct {
	RecordEntries uint64 `protobuf:"varint,1,opt,name=record_entries,json=recordEntries,proto3" json:"record_entries,omitempty"`
	TimestampFrom uint64 `protobuf:"varint,2,opt,name=timestamp_from,json=timestampFrom,proto3" json:"timestamp_from,omitempty"`
	TimestampTo   uint64 `protobuf:"varint,3,opt,name=timestamp_to,json=timestampTo,proto3" json:"timestamp_to,omitempty"`
	DeltaLogPath  string `protobuf:"bytes,4,opt,name=delta_log_path,json=deltaLogPath,proto3" json:"delta_log_path,omitempty"`
	DeltaLogSize  int64  `protobuf:"varint,5,opt,name=delta_log_size,json=deltaLogSize,proto3" json:"delta_log_size,omitempty"`
}
