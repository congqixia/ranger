package util

import (
	"os"
	"path/filepath"
	"strings"
)

type FindLogMode int

const (
	Auto FindLogMode = iota
	Standalone
	Distributed
	Flat
)

// FindLogs returns milvus log file path under provided root path.
func FindLogs(path string, mode FindLogMode) []string {
	switch mode {
	case Auto:
	case Standalone:
	case Distributed:
	}

	var paths []string

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".gz") || strings.HasSuffix(path, ".tmp") {
			return nil
		}

		switch {
		case strings.Contains(path, "/datacoord/"):
			fallthrough
		case strings.Contains(path, "/datanode/"):
			fallthrough
		case strings.Contains(path, "/proxy/"):
			fallthrough
		case strings.Contains(path, "/rootcoord/"):
			fallthrough
		case strings.Contains(path, "/querycoord/"):
			fallthrough
		case strings.Contains(path, "/querynode/"):
			fallthrough
		case strings.Contains(path, "/indexcoord/"):
			fallthrough
		case strings.Contains(path, "/indexnode/"):
			if strings.HasSuffix(path, ".gz") || strings.HasSuffix(path, ".tmp") {
				return nil
			}
			paths = append(paths, path)
		default:
			if mode == Flat && strings.Contains(path, ".log") {
				paths = append(paths, path)
			}
		}

		return nil
	})
	if err != nil {
		return nil
	}

	return paths
}
