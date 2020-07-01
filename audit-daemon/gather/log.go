package gather

import (
	"encoding/json"
)

const (
	CREATE  = 0
	APPEND  = 1
	RENAME  = 2
	ARCHIVE = 3
)

type LogItem struct {
	OpType   int    `json:"type"`
	Inode    uint64 `json:"inode"`
	Src      string `json:"src"`
	Dir      string `json:"dir"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	Time     string `json:"time"`
}

func convertLogStrToJson(line string) (logItem *LogItem, err error) {
	logItem = &LogItem{}
	if err = json.Unmarshal([]byte(line), logItem); err != nil {
		return
	}
	return
}
