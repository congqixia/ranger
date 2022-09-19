package processors

import (
	"fmt"
	"strings"
	"time"

	"github.com/congqixia/ranger/parser"
	"github.com/golang/protobuf/proto"
)

const (
	execMergePlan          = `exec merge compaction plan`
	completeSegmentRequest = `receive complete compaction request`
	successCompletePlan    = `success to complete compaction`
)

type CompactionProcessor struct {
	Plans       []CompactionPlanRecord
	PlanIDEntry map[int64]CompactionPlanResult
}

type CompactionPlanRecord struct {
	PlanID     int64
	StartTime  time.Time
	Channel    string
	SegmentIDs []int64
}

type CompactionPlanResult struct {
	SegmentID int64
	EndTime   time.Time
	Success   bool
}

func (p *CompactionProcessor) ProcessEntry(entry *parser.Entry) {
	if p.PlanIDEntry == nil {
		p.PlanIDEntry = make(map[int64]CompactionPlanResult)
	}
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
	case completeSegmentRequest:
		planID, has := entry.SearchDataInt64("planID")
		if !has {
			break
		}
		segmentID, has := entry.SearchDataInt64("segmentID")
		if !has {
			break
		}
		result := p.PlanIDEntry[planID]
		result.SegmentID = segmentID
		p.PlanIDEntry[planID] = result

	case successCompletePlan:
		planID, has := entry.SearchDataInt64("planID")
		if !has {
			break
		}
		result := p.PlanIDEntry[planID]
		result.EndTime = entry.TS
		result.Success = true
		p.PlanIDEntry[planID] = result

	default:
		return
	}
	entry.Processed = true
}
