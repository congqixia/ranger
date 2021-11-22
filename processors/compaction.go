package processors

import (
	"fmt"
	"strings"
	"time"

	"github.com/congqixia/milvus-log-parser/parser"
	"github.com/golang/protobuf/proto"
)

const (
	execMergePlan = `exec merge compaction plan`
)

type CompactionProcessor struct {
	Plans []CompactionPlanRecord
}

type CompactionPlanRecord struct {
	PlanID     int64
	StartTime  time.Time
	Channel    string
	SegmentIDs []int64
}

func (p *CompactionProcessor) ProcessEntry(entry *parser.Entry) {
	switch entry.Msg {
	case execMergePlan:
		planRaw, has := entry.SearchData("plan")
		if !has {
			break
		}
		// remove surrounding ""
		planRaw = planRaw[1 : len(planRaw)-1]
		planRaw = strings.Replace(planRaw, "\\", "", -1)
		plan := &CompactionPlan{}
		err := proto.UnmarshalText(planRaw, plan)
		if err != nil {
			fmt.Println(err.Error())
		} else {
			var segmentIDs []int64
			for _, binlog := range plan.GetSegmentBinlogs() {
				segmentIDs = append(segmentIDs, binlog.GetSegmentID())
			}
			p.Plans = append(p.Plans, CompactionPlanRecord{
				PlanID:     plan.GetPlanID(),
				Channel:    plan.GetChannel(),
				StartTime:  entry.TS,
				SegmentIDs: segmentIDs,
			})
		}
	default:
		return
	}
	entry.Processed = true
}
