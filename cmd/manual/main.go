package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/congqixia/milvus-log-parser/parser"
	"github.com/congqixia/milvus-log-parser/processors"
	"github.com/congqixia/milvus-log-parser/util"
)

var (
	logRoot          = flag.String("log", "", "Log file root")
	logMode          = flag.String("mode", "auto", "Log directory file mode, default auto")
	collectionID     = flag.Int64("collid", 0, "Collection id to inspect")
	collName         = flag.String("collname", "", "Collection name to inspect")
	printRemain      = flag.Bool("remain", false, "Print log not processed")
	printRemainLimit = flag.Int64("remain-limit", 10, "Print log not processed minimal occurs times")
)

func main() {
	flag.Parse()

	if *logRoot == "" {
		log.Fatal("log root not provided")
	}

	mode := util.Auto
	switch strings.ToLower(*logMode) {
	case "flat":
		mode = util.Flat
	default:
	}
	filePaths := util.FindLogs(*logRoot, mode)

	flushProcessor := &processors.FlushProcessor{}
	getRecoveryInfoProessor := &processors.GetRecoveryInfoProcessor{}
	collProcessor := &processors.CollectionLifeCircleProcessor{}
	loadCollectionProcessor := &processors.LoadCollectionProcessor{}
	compactionProcessor := &processors.CompactionProcessor{}
	remainProcessor := &processors.RemainCounter{}
	for _, filePath := range filePaths {
		file, err := os.Open(filePath)
		if err != nil {
			log.Println("failed to open file", err.Error())
			os.Exit(2)
		}

		scanner := bufio.NewScanner(file)
		p := &parser.ZapTextParser{}
		l := 0

		var logEntry parser.Entry
		for scanner.Scan() {
			l++
			logEntry = p.ParseLine(scanner.Bytes())
			flushProcessor.ProcessEntry(&logEntry)
			collProcessor.ProcessEntry(&logEntry)
			getRecoveryInfoProessor.ProcessEntry(&logEntry)
			loadCollectionProcessor.ProcessEntry(&logEntry)
			compactionProcessor.ProcessEntry(&logEntry)
			remainProcessor.ProcessEntry(&logEntry)
		}
		file.Close()

	}

	zeroCount := 0
	for _, ele := range collProcessor.Info {
		if ele.CreatedAt.IsZero() || ele.DropedAt.IsZero() {
			zeroCount++
			continue
		}
		//fmt.Printf("collection %s start - end: %v-%v duration: %v\n", collection, ele.CreatedAt, ele.DropedAt, ele.DropedAt.Sub(ele.CreatedAt))
	}
	fmt.Println("zero count:", zeroCount)

	var targets []int64

	if *collectionID > 0 {
		fmt.Println("Add collection id to target:", *collectionID)
		targets = append(targets, *collectionID)
	}

	if *collName != "" {
		id, has := collProcessor.Name2ID[*collName]
		if has {
			targets = append(targets, id)
		}
	}
	fmt.Println("filtering with targets:", targets)

	for coll, info := range flushProcessor.Info {
		found := false
		for _, target := range targets {
			if target == coll {
				fmt.Println("found in flush")
				found = true
				break
			}
		}
		if !found && len(targets) > 0 {
			fmt.Println(coll, "skipped")
			continue
		}
		sort.Slice(info.Events, func(i, j int) bool {
			return info.Events[i].EventTS().Before(info.Events[j].EventTS())
		})

		fmt.Println("collection:", coll)
		for _, event := range info.Events {
			fmt.Println(event.Display())
		}
	}

	sort.Slice(getRecoveryInfoProessor.Records, func(i, j int) bool {
		return getRecoveryInfoProessor.Records[i].Ts.Before(getRecoveryInfoProessor.Records[j].Ts)
	})
	for _, record := range getRecoveryInfoProessor.Records {
		found := false
		for _, target := range targets {
			if target == record.CollectionID {
				found = true
				break
			}
		}
		if !found && len(targets) > 0 {
			continue
		}

		fmt.Printf("[%v]%s: %v\n", record.Ts, record.RecordType, record.CollectionID)
	}
	sort.Slice(loadCollectionProcessor.Records, func(i, j int) bool {
		return loadCollectionProcessor.Records[i].Ts.Before(loadCollectionProcessor.Records[j].Ts)
	})

	for _, record := range loadCollectionProcessor.Records {
		fmt.Printf("[%v]%s: %v, detail: %v\n", record.Ts, record.RecordType, record.CollectionID, record.Extra)
	}

	for _, plan := range compactionProcessor.Plans {
		fmt.Printf("[%v]Plan: %d(Channel:%s), compact segments:%v\n", plan.StartTime, plan.PlanID, plan.Channel, plan.SegmentIDs)
	}

	if *printRemain {
		for key, cnt := range remainProcessor.Counter {
			if cnt < *printRemainLimit {
				continue
			}
			fmt.Printf("%s: %d\n", key, cnt)
		}
	}
}
