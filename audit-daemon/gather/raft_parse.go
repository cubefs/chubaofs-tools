package gather

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	. "github.com/chubaofs/chubaofs-tools/audit-daemon/util"
	"github.com/chubaofs/chubaofs-tools/audit-daemon/util/raft"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"syscall"
	"time"
)

const (
	opFSMCreateInode uint32 = iota
	opFSMUnlinkInode
	opFSMCreateDentry
	opFSMDeleteDentry
	opFSMDeletePartition
	opFSMUpdatePartition
	opFSMDecommissionPartition
	opFSMExtentsAdd
	opFSMStoreTick
	startStoreTick
	stopStoreTick
	opFSMUpdateDentry
	opFSMExtentTruncate
	opFSMCreateLinkInode
	opFSMEvictInode
	opFSMInternalDeleteInode
	opFSMSetAttr
	opFSMInternalDelExtentFile
	opFSMInternalDelExtentCursor
	opExtentFileSnapshot
	opFSMSetXAttr
	opFSMRemoveXAttr
	opFSMCreateMultipart
	opFSMRemoveMultipart
	opFSMAppendMultipart
	opFSMSyncCursor

	//supplement action
	opFSMInternalDeleteInodeBatch
	opFSMDeleteDentryBatch
	opFSMUnlinkInodeBatch
	opFSMEvictInodeBatch
)

const (
	OffsetMetaFile = "offset_meta"
	MetaFile       = "meta"
	ArchiveDir     = "archive"
)

type OpKvData struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

var chubaodbAddr string
var offsetMeta map[uint64]int64 // record offset of log file and raft file: inode -> offset (has read)

func StartRaftParse(logDir string, dbAddr string) (err error) {

	if _, err = os.Stat(logDir); os.IsNotExist(err) {
		if err = os.MkdirAll(logDir, os.ModePerm); err != nil {
			return err
		}
	}

	chubaodbAddr = dbAddr
	var logFileInfos []*FileInfo
	for {
		if Stop {
			break
		}
		if offsetMeta, logFileInfos, err = loadSyncLogFile(logDir); err != nil {
			LOG.Errorf("load sync log files err: [%v], dir [%v]", err, logDir)
			return err
		}
		for _, logFileInfo := range logFileInfos {
			var (
				file        *os.File
				startOffset int64
				exist       bool
			)
			if file, err = os.Open(path.Join(logDir, logFileInfo.Name)); err != nil {
				LOG.Errorf("start parse raft files: open log file [%v] err [%v]", path.Join(logDir, logFileInfo.Name), err)
				return err
			}
			if startOffset, exist = offsetMeta[logFileInfo.Inode]; !exist {
				startOffset = 0
			}
			if err = readLogFile(file, startOffset); err != nil && err != io.EOF {
				LOG.Errorf("start parse raft files: read log file [%v] err [%v]", path.Join(logDir, logFileInfo.Name), err)
				return err
			}
			offsetMeta[logFileInfo.Inode] = logFileInfo.Size //record next read offset. todo: read bytes len
			_ = file.Close()
		}
		WriteMeta(path.Join(logDir, OffsetMetaFile), offsetMeta)

		time.Sleep(10 * time.Second)

	}
	return nil
}

func loadSyncLogFile(dir string) (map[uint64]int64, []*FileInfo, error) {
	LOG.Debugf("list sync log files: dir[%v]", dir)

	offsetMeta := make(map[uint64]int64)
	file, err := os.Open(path.Join(dir, OffsetMetaFile))
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, err
		}
	} else {
		defer func() {
			_ = file.Close()
		}()
		bs, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal(bs, &offsetMeta); err != nil {
			return nil, nil, err
		}
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	var list []*FileInfo

	for _, f := range files {
		stat, ok := f.Sys().(*syscall.Stat_t)
		if !ok {
			panic("Not a syscall.Stat_t")
		}

		if f.IsDir() {
			continue
		}

		if f.Name() == OffsetMetaFile || f.Name() == MetaFile {
			continue
		}

		list = append(list, &FileInfo{
			Inode: stat.Ino,
			Name:  f.Name(),
			Size:  f.Size(),
			Time:  f.ModTime(),
		})
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].Time.Unix() < list[j].Time.Unix() {
			return true
		}
		return false
	})

	return offsetMeta, list, nil
}

func readLogFile(file *os.File, startOffset int64) (err error) {
	if _, err = file.Seek(startOffset, io.SeekStart); err != nil {
		LOG.Errorf("read log file: seek file[%s] err[%s]", file.Name(), err.Error())
		return
	}
	strReader := bufio.NewReader(file)
	var line []byte
	for {
		line, _, err = strReader.ReadLine()
		if err != nil || io.EOF == err {
			break
		}
		var logItem *LogItem
		if logItem, err = convertLogStrToJson(string(line)); err == nil {
			if logItem.OpType != RENAME {
				if err = parseLogItem(logItem); err != nil {
					LOG.Errorf("find raft log file: inode[%s] err[%s]", logItem.Inode, err.Error())
					return
				}
			}
		}
	}
	return
}

func parseLogItem(logItem *LogItem) (err error) {
	var (
		file     *os.File
		filePath string
	)
	fileDir := logItem.Dir
	if filePath, err = findFileByInode(fileDir, logItem.Inode); err != nil {
		LOG.Errorf("read raft file: find file inode[%v], dir[%v], err[%v]", logItem.Inode, logItem.Dir, err)
		return
	}
	if filePath == "" { // if file is moved to archive
		fileDir = path.Join(logItem.Dir, ArchiveDir)
		filePath, err = findFileByInode(fileDir, logItem.Inode)
		if err != nil || filePath == "" {
			LOG.Errorf("read raft file: find file inode[%v], dir[%v], err[%v]", logItem.Inode, path.Join(logItem.Dir, "archive"), err)
			return err
		}
	}
	if file, err = os.Open(filePath); err != nil {
		LOG.Errorf("read raft file: open file[%v], err[%v]", filePath, err)
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	var (
		endOffset   int64
		startOffset int64
		exist       bool
	)
	if startOffset, exist = offsetMeta[logItem.Inode]; !exist {
		startOffset = 0
	}
	if endOffset, err = readRaftFile(file, logItem.Inode, startOffset, ipSyncMap[logItem.Dir]); err != nil {
		LOG.Errorf("read raft file: file [%v] offset [%v] err [%v]", path.Join(fileDir, file.Name()), startOffset, err)
		return err
	}
	if logItem.OpType == ARCHIVE && endOffset == startOffset { // file was deleted
		delete(offsetMeta, logItem.Inode)
		return
	}
	return
}

func readRaftFile(file *os.File, inode uint64, startOffset int64, ip string) (endOffset int64, err error) {
	LOG.Debugf("start reading raft log file: [%v]", file.Name())
	var readLen int
	var readOffset int64
	endOffset = startOffset
	for {
		if _, err = file.Seek(endOffset, io.SeekStart); err != nil {
			LOG.Errorf("read log file: seek file[%s] err[%s]", file.Name(), err.Error())
			return
		}
		readBytes := make([]byte, BodySize)
		if readLen, err = file.Read(readBytes); err != nil && err != io.EOF {
			LOG.Errorf("parse raft files: read err[%s], file[%s]", err.Error(), file.Name())
			return
		}
		if readOffset, err = parseRaftItem(readBytes[:readLen], inode, endOffset, ip); err != nil {
			LOG.Errorf("parse raft files: parse err[%s], file[%s]", err.Error(), file.Name())
			return
		}
		if readOffset <= 0 {
			break
		}
		endOffset = endOffset + readOffset
	}
	return endOffset, nil
}

func parseRaftItem(data []byte, inode uint64, startOffset int64, ip string) (readOffset int64, err error) {
	LOG.Debug("Read raft wal record: data len [%v]", len(data))
	var (
		fileOffset uint64
		dataSize   uint64
		//dataString string
		//randWrite  *rndWrtItem
	)

	fileOffset = 0

	for {
		if fileOffset+1+8 >= uint64(len(data)) {
			break
		}

		dataSize = 0
		//dataString = ""
		dataTemp := data[fileOffset:]
		tempLen := len(dataTemp)
		//first byte is record type
		recordType := dataTemp[0]
		//sencond 8 bytes is datasize
		dataSize = binary.BigEndian.Uint64(dataTemp[1:9])
		if 1+8+dataSize+4 > uint64(tempLen) {
			break
		}
		//third byte is op type
		opType := dataTemp[9]
		//and 8+8 types is term and index
		term := binary.BigEndian.Uint64(dataTemp[10:18])
		index := binary.BigEndian.Uint64(dataTemp[18:26])
		var valBytes []byte
		var raftItemMap map[string]interface{}
		if dataSize > 17 {
			if opType == 0 {
				cmd := new(OpKvData)
				if err = json.Unmarshal(dataTemp[26:26+dataSize-17], cmd); err != nil {
					LOG.Errorf("unmarshal fail: err[%v]", err)
					return
				}
				if raftItemMap, err = parseMetaOp(cmd); err != nil {
					LOG.Errorf("parse meta operation err: cmd[%v], err[%v]", cmd, err)
					return
				}
			} else if opType == 1 {
				raftItemMap = make(map[string]interface{})
				raftItemMap["cngType"] = dataTemp[26]
				raftItemMap["peerType"] = dataTemp[27]
				raftItemMap["priority"] = binary.BigEndian.Uint16(dataTemp[28:30])
				raftItemMap["peerId"] = binary.BigEndian.Uint64(dataTemp[30:38])
			}

		} else {
			raftItemMap = make(map[string]interface{})
			raftItemMap["data"] = dataTemp[9 : 9+dataSize]
		}
		crcOffset := 1 + 8 + dataSize
		crc := binary.BigEndian.Uint32(dataTemp[crcOffset : crcOffset+4])
		addRaftItemMap(raftItemMap, recordType, dataSize, opType, term, index, crc)
		raftItemMap["NodeIP"] = ip
		if valBytes, err = json.Marshal(raftItemMap); err != nil {
			LOG.Errorf("cmd single map value json marshal error: [%v], map[%v]", err, raftItemMap)
			return
		}
		if err = sendRaftItem(inode, uint64(startOffset)+fileOffset, valBytes); err != nil {
			LOG.Errorf("send raft item to database err: [%v], inode[%v], offset[%v], body[%v]", err, inode, uint64(startOffset)+fileOffset, valBytes)
			return
		}

		recordSize := 1 + 8 + dataSize + 4 // the length of an item
		fileOffset = fileOffset + recordSize
		offsetMeta[inode] = startOffset + int64(fileOffset) // update meta record
	}
	return int64(fileOffset), nil
}

func parseMetaOp(cmd *OpKvData) (raftItemMap map[string]interface{}, err error) {
	switch cmd.Op {
	case opFSMCreateInode, opFSMExtentsAdd, opFSMExtentTruncate, opFSMCreateLinkInode, opFSMEvictInode, opFSMUnlinkInode:
		if raftItemMap, err = parseInode(cmd); err != nil {
			LOG.Errorf("parse raft item: parse inode fail: err[%v]", err)
			return
		}
	case opFSMCreateDentry, opFSMDeleteDentry, opFSMDeleteDentryBatch, opFSMUpdateDentry:
		if raftItemMap, err = parseDentry(cmd); err != nil {
			LOG.Errorf("parse raft item: parse dentry fail: err[%v]", err)
			return
		}
	case opFSMInternalDeleteInodeBatch, opFSMEvictInodeBatch, opFSMUnlinkInodeBatch:
		if raftItemMap, err = parseInodeBatch(cmd); err != nil {
			LOG.Errorf("parse raft item: parse inode batch fail: err[%v]", err)
			return
		}
		//todo check
	case opFSMSyncCursor:
		cursor := binary.BigEndian.Uint64(cmd.V)
		raftItemMap = make(map[string]interface{})
		raftItemMap["cursor"] = cursor
	case opFSMInternalDeleteInode:
		var inodeIDs []uint64
		if inodeIDs, err = raft.InternalDeleteInode(cmd.V); err != nil {
			LOG.Errorf("parse raft item: inode batch unmarshal err[%v], cmd[%v]", err, cmd)
			return
		}
		raftItemMap = make(map[string]interface{})
		raftItemMap["InodeIDs"] = inodeIDs
	case opFSMInternalDelExtentFile:
		raftItemMap = make(map[string]interface{})
		raftItemMap["filename"] = string(cmd.V)
	case opFSMUpdatePartition, opFSMSetAttr:
		var values map[string]interface{}
		if err = json.Unmarshal(cmd.V, &values); err != nil {
			LOG.Errorf("cmd value json unmarshal error: [%v], cmd[%v]", err, cmd)
			return
		}
		raftItemMap = DrawMap(values, ".")
		//todo check
	case opFSMSetXAttr, opFSMRemoveXAttr:
		var extend *raft.Extend
		if extend, err = raft.NewExtendFromBytes(cmd.V); err != nil {
			LOG.Errorf("cmd value extend unmarshal error: [%v], cmd[%v]", err, cmd)
			return
		}
		raftItemMap = make(map[string]interface{})
		raftItemMap["Inode"] = extend.Inode
		for k, v := range extend.DataMap {
			raftItemMap["dataMap."+k] = string(v)
		}
	case opFSMAppendMultipart, opFSMCreateMultipart, opFSMRemoveMultipart:
		var multipart *raft.Multipart
		multipart = raft.MultipartFromBytes(cmd.V)
		raftItemMap = make(map[string]interface{})
		raftItemMap["id"] = multipart.Id
		raftItemMap["key"] = multipart.Key
		raftItemMap["initTime"] = multipart.InitTime
		partStr := make([]string, 0)
		for _, part := range multipart.Parts {
			var partBytes []byte
			if partBytes, err = json.Marshal(part); err != nil {
				LOG.Errorf("parse raft item: unmarshal multipart fail: err[%v]", err)
				return
			}
			partStr = append(partStr, string(partBytes))
		}
		raftItemMap["parts"] = partStr
	case opFSMInternalDelExtentCursor:
		str := string(cmd.V)
		var fileName string
		var cursor int64
		if _, err = fmt.Sscanf(str, "%s %d", &fileName, &cursor); err != nil {
			LOG.Errorf("cmd value scanf err: [%v], cmd[%v]", err, cmd)
			return
		}
		raftItemMap = make(map[string]interface{})
		raftItemMap["filename"] = fileName
		raftItemMap["cursor"] = cursor
		//todo check
	case opFSMStoreTick:
		// data is null
		raftItemMap = make(map[string]interface{})
	default:
		LOG.Errorf("unsupported op: [%v]", cmd.Op)
		raftItemMap = make(map[string]interface{})
		raftItemMap["data"] = cmd.V
	}
	//dataString := fmt.Sprintf("opt:%v, k:%v, v:%v", cmd.Op, cmd.K, string(bytes))
	raftItemMap["op_2"] = cmd.Op
	raftItemMap["key_2"] = cmd.K
	return
}

func sendRaftItem(inodeID uint64, offset uint64, body []byte) (err error) {
	url := "http://" + chubaodbAddr + "/" + strconv.FormatUint(inodeID, 10) + "_" + strconv.FormatUint(offset, 10)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		LOG.Errorf("send raft item request[%s]: new request err: [%s]", url, err.Error())
		return
	}

	do, err := http.DefaultClient.Do(req)
	if err != nil {
		LOG.Errorf("send raft item request[%s]: do client err: [%s]", url, err.Error())
		return
	}

	if do.StatusCode != 200 {
		resBody := do.Body
		buf := new(bytes.Buffer)
		buf.ReadFrom(resBody)
		LOG.Warningf("send request[%s]: status code: [%v]", url, do.StatusCode)
		return fmt.Errorf("post has status:[%d] body:[%s]", do.StatusCode, buf.String())
	}

	all, err := ioutil.ReadAll(do.Body)
	_ = do.Body.Close()
	if err != nil {
		LOG.Errorf("send raft item request[%s]: read body err: [%s]", url, err.Error())
		return err
	}

	var resp Response
	if err = json.Unmarshal(all, &resp); err != nil {
		LOG.Errorf("send raft item request[%s]: unmarshal err: [%s]", url, err.Error())
		return err
	}

	if resp.Code > 200 {
		LOG.Warningf("send raft item request[%s]: response code: [%v]", url, resp.Code)
		return fmt.Errorf(resp.Msg)
	}

	return nil
}

func addRaftItemMap(raftItemMap map[string]interface{}, recordType byte, dataSize uint64, opType byte, term uint64, index uint64, crc uint32) {
	raftItemMap["recType_1"] = recordType
	raftItemMap["dataSize_1"] = dataSize
	raftItemMap["opType_1"] = opType
	raftItemMap["term_1"] = term
	raftItemMap["index_1"] = index
	raftItemMap["crc_1"] = crc
}

func findFileByInode(dir string, inode uint64) (filePath string, err error) {
	var fileInfos []os.FileInfo
	if fileInfos, err = ioutil.ReadDir(dir); err != nil {
		LOG.Errorf("find file by inode: read dir[%v], err[%v]", dir, err)
		return
	}
	for _, fileInfo := range fileInfos {
		stat, ok := fileInfo.Sys().(*syscall.Stat_t)
		if !ok {
			LOG.Errorf("read raft file: dir[%v], file[%v], stat err", dir, fileInfo.Name())
			return "", fmt.Errorf("file stat err")
		}
		if stat.Ino == inode {
			filePath = path.Join(dir, fileInfo.Name())
			return
		}
	}
	return
}
