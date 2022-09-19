package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/congqixia/ranger/parser"
	"github.com/congqixia/ranger/processors"
	"github.com/congqixia/ranger/util"
)

var (
	logRoot = flag.String("log", "", "Log file root")
	logMode = flag.String("mode", "auto", "Log directory file mode, default auto")
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
	fmt.Println("filePath:", filePaths)

	plan := processors.ProcessPlan{
		Items: []processors.LogItem{
			{
				Short: "QueryWaitFailed",
				Tag:   "Query failed to WaitToFinish",
				Attributes: []processors.LogAttr{
					{
						Tag:  "traceID",
						Type: processors.AttrTypeString,
					},
					{
						Tag:  "error",
						Type: processors.AttrTypeString,
					},
					{
						Tag:  "collection",
						Type: processors.AttrTypeString,
					},
					{
						Tag:  "msgID",
						Type: processors.AttrTypeInt64,
					},
				},
			},
			{
				Short: "CollectionIDName",
				Tag:   "add collection to meta table",
				Attributes: []processors.LogAttr{
					{
						Tag:  "id",
						Type: processors.AttrTypeInt64,
					},
					{
						Tag:  "collection",
						Type: processors.AttrTypeString,
					},
				},
			},
		},
	}

	pp := &processors.PlanProcessor{
		Plan: plan,
	}

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
			pp.ProcessEntry(&logEntry)
		}
		file.Close()
	}

	for _, result := range pp.Results {
		fmt.Printf("%#v\n", result)
	}
}
