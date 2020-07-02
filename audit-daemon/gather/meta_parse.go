package gather

import (
	"encoding/json"
	. "github.com/chubaofs/chubaofs-tools/audit-daemon/util"
	"github.com/chubaofs/chubaofs-tools/audit-daemon/util/raft"
)

func parseInode(cmd *OpKvData) (raftItemMap map[string]interface{}, err error) {
	ino := raft.NewInode(0, 0)
	if err = ino.Unmarshal(cmd.V); err != nil {
		LOG.Errorf("parse raft item: inode unmarshal err[%v], cmd[%v]", err, cmd)
		return
	}
	var inodeBytes []byte
	if inodeBytes, err = json.Marshal(ino); err != nil {
		LOG.Errorf("parse raft item: inode marshal err[%v], inode[%v]", err, ino)
		return
	}
	var values map[string]interface{}
	if err = json.Unmarshal(inodeBytes, &values); err != nil {
		LOG.Errorf("cmd value json unmarshal error: [%v], cmd[%v]", err, cmd)
		return
	}
	raftItemMap = DrawMap(values, ".")
	raftItemMap["LinkTarget"] = string(ino.LinkTarget)
	return
}

func parseInodeBatch(cmd *OpKvData) (raftItemMap map[string]interface{}, err error) {
	var inodeBatch raft.InodeBatch
	if inodeBatch, err = raft.InodeBatchUnmarshal(cmd.V); err != nil {
		LOG.Errorf("parse raft item: inode batch unmarshal err[%v], cmd[%v]", err, cmd)
		return
	}
	inodeBatchStr := make([]string, 0)
	var inodeBytes []byte
	for _, curInode := range inodeBatch {
		if inodeBytes, err = json.Marshal(curInode); err != nil {
			LOG.Errorf("parse raft item: inode batch marshal err[%v], inode[%v]", err, curInode)
			return
		}
		var values map[string]interface{}
		if err = json.Unmarshal(inodeBytes, &values); err != nil {
			LOG.Errorf("cmd value json unmarshal error: [%v], cmd[%v]", err, cmd)
			return
		}
		inodeMap := DrawMap(values, ".")
		inodeMap["LinkTarget"] = string(curInode.LinkTarget)
		var inodeMapBytes []byte
		if inodeMapBytes, err = json.Marshal(inodeMap); err != nil {
			LOG.Errorf("inode map value json marshal error: [%v], map[%v]", err, raftItemMap)
			return
		}
		inodeBatchStr = append(inodeBatchStr, string(inodeMapBytes))
	}
	raftItemMap = make(map[string]interface{})
	raftItemMap["InodeBatch"] = inodeBatchStr
	return
}

func parseDentry(cmd *OpKvData) (raftItemMap map[string]interface{}, err error) {
	den := &raft.Dentry{}
	if err = den.Unmarshal(cmd.V); err != nil {
		LOG.Errorf("parse raft item: dentry unmarshal err[%v], cmd[%v]", err, cmd)
		return
	}
	var denBytes []byte
	if denBytes, err = json.Marshal(den); err != nil {
		LOG.Errorf("parse raft item: dentry marshal err[%v], dentry[%v]", err, den)
		return
	}
	var values map[string]interface{}
	if err = json.Unmarshal(denBytes, &values); err != nil {
		LOG.Errorf("cmd value json unmarshal error: [%v], cmd[%v]", err, cmd)
		return
	}
	raftItemMap = DrawMap(values, ".")
	return
}
