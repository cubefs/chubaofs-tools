package gather

import (
	"bytes"
	"encoding/json"
	"fmt"
	. "github.com/chubaofs/chubaofs-tools/audit-daemon/util"
	"io"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

var ipSyncMap map[string]string // key: The path to store the synchronization file, value: ip of machine which did the file come from

type Worker struct {
	addr string
	jobs []*Job
}

type MyError struct {
	error
	name string
}

func (e *MyError) Error() string {
	return e.name + " , " + e.error.Error()
}

func newErr(name string, e error) *MyError {
	return &MyError{
		error: e,
		name:  name,
	}
}

//this function will run background not block
func StartGather(configPath string) {
	parseConfig(configPath)
	for _, w := range workers {
		go func(w *Worker) {
			toWork(w)
		}(w)
	}
	LOG.Debug("all workers has stopped")
}

type Job struct {
	src     string
	dist    string
	pattern string
}

var workers = make(map[string]*Worker)

func toWork(w *Worker) {

	for {
		if Stop {
			break
		}
		for _, job := range w.jobs {
			w.toJob(job)
		}

		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) toJob(job *Job) {
	LOG.Debugf("start job: addr[%v], src[%v], dist[%v], pattern[%v]", w.addr, job.src, job.dist, job.pattern)

	remoteFiles, err := w.remoteFiles(job.src, job.pattern)
	if err != nil {
		LOG.Errorf("list remote files has err:[%s]", err.Error())
		return
	}

	meta, localFiles, err := w.localFiles(job.dist, job.pattern)
	if err != nil {
		LOG.Errorf("list local files has err:[%s]", err.Error())
		return
	}

	localMap := make(map[uint64]bool)
	for _, lf := range localFiles {
		localMap[lf.Inode] = true
	}

	for _, rfile := range remoteFiles {
		if linode, found := meta[rfile.Inode]; found {
			delete(localMap, linode)
			if lfile := findByInode(linode, localFiles); lfile != nil {
				if err = job.appendFile(w, meta, lfile, rfile); err != nil {
					LOG.Errorf("append file:[%s],[%s] has err:[%s]", job.dist, lfile.Name, err.Error())
				}
				continue
			}
		}
		if err := job.createFile(w, meta, rfile); err != nil {
			LOG.Errorf("create file:[%s],[%s] has err:[%s]", job.dist, rfile.Name, err.Error())
		}
	}

	for linode := range localMap {
		if lfile := findByInode(linode, localFiles); lfile != nil {
			if err := os.Rename(path.Join(job.dist, lfile.Name), path.Join(job.dist, "archive", lfile.Name)); err != nil {
				LOG.Errorf("rname:[%s],[%s] has err:[%s]", job.dist, lfile.Name, err.Error())
			} else {
				LOG.Info(formatJsonLogMessage(ARCHIVE, linode, job.dist, path.Join(job.dist, "archive"), lfile.Name, 0))
			}
		}
	}

	for rinode := range meta {
		if findByInode(rinode, remoteFiles) == nil {
			delete(meta, rinode)
		}
	}

	WriteMeta(path.Join(job.dist, MetaFile), meta)
}

func (j *Job) appendFile(w *Worker, meta map[uint64]uint64, local, remote *FileInfo) error {
	LOG.Debugf("start to append file: addr[%v], remote file[%v], local file[%v]", w.addr, remote.Name, local.Name)

	if local.Size != remote.Size {
		if local.Size > remote.Size {
			return j.createFile(w, meta, remote)
		}

		lfile, err := os.OpenFile(path.Join(j.dist, local.Name), os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			return newErr("open local file", err)
		}
		defer func() {
			if err := lfile.Close(); err != nil {
				LOG.Errorf("close remote file has err:[%s]", err.Error())
			}
		}()

		var start = local.Size
		for {
			respData, err := Send(w.addr+PathReadFile, &RequestGetContent{
				Dir:     j.src,
				Name:    remote.Name,
				Pattern: j.pattern,
				Inode:   remote.Inode,
				Start:   start,
			})
			if err != nil {
				return err
			}
			respLen := int64(len(respData))
			if respLen == 0 {
				break
			}

			if _, err = io.Copy(lfile, bytes.NewReader(respData)); err != nil {
				LOG.Errorf("append file: copy file dst[%s] err: [%s]", lfile.Name(), err.Error())
				return err
			}
			start = start + respLen

			if respLen < BodySize {
				break
			}
		}
		LOG.Infof(formatJsonLogMessage(APPEND, local.Inode, j.src, j.dist, local.Name, remote.Size-local.Size))
		return nil
	}

	if remote.Name != local.Name {
		LOG.Info(formatJsonLogMessage(RENAME, local.Inode, path.Join(j.dist, local.Name), j.dist, remote.Name, 0))
		return os.Rename(path.Join(j.dist, local.Name), path.Join(j.dist, remote.Name))
	}

	return nil
}

func (j *Job) createFile(w *Worker, meta map[uint64]uint64, remote *FileInfo) error {
	LOG.Debugf("start to create file: addr[%v], file[%v]", w.addr, remote.Name)

	newFilePath := path.Join(j.dist, remote.Name)

	if f, err := os.Stat(newFilePath); err == nil { //means file exists
		stat, ok := f.Sys().(*syscall.Stat_t)
		if ok {
			for _, l := range meta {
				if l == stat.Ino {
					LOG.Debugf("skip create file %s", newFilePath)
					return nil
				}
			}
		}
	}

	lfile, err := os.OpenFile(newFilePath, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		LOG.Errorf("create file: open file[%s] err: [%s]", newFilePath, err.Error())
		return err
	}
	defer func() {
		_ = lfile.Close()
	}()

	var start int64
	for {
		respData, err := Send(w.addr+PathReadFile, &RequestGetContent{
			Dir:     j.src,
			Name:    remote.Name,
			Pattern: j.pattern,
			Inode:   remote.Inode,
			Start:   start,
		})
		if err != nil {
			LOG.Errorf("create file: send read file request[%s] err: [%s]", remote.Name, err.Error())
			return err
		}
		respLen := int64(len(respData))
		if respLen == 0 {
			break
		}

		if _, err = io.Copy(lfile, bytes.NewReader(respData)); err != nil {
			LOG.Errorf("create file: copy file dst[%s] err: [%s]", lfile.Name(), err.Error())
			return err
		}
		start = start + respLen

		if respLen < BodySize {
			break
		}
	}

	if err = lfile.Sync(); err != nil {
		LOG.Errorf("create file: sync err: [%s]", err.Error())
		return err
	}

	f, err := os.Stat(newFilePath)
	if err != nil {
		LOG.Errorf("create file: stat file err: [%s]", err.Error())
		return err
	}

	stat, ok := f.Sys().(*syscall.Stat_t)
	if !ok {
		panic("Not a syscall.Stat_t")
	}

	meta[remote.Inode] = stat.Ino

	LOG.Info(formatJsonLogMessage(CREATE, stat.Ino, "", j.dist, remote.Name, 0))

	if err = os.Chtimes(newFilePath, remote.Time, remote.Time); err != nil {
		LOG.Errorf("change create file name has err:[%s]", err.Error()) //todo need return err?
	}

	return nil
}

func findByInode(inode uint64, files []*FileInfo) *FileInfo {
	for _, f := range files {
		if f.Inode == inode {
			return f
		}
	}
	return nil
}

// meta: remoteInode -> localInode
func (w *Worker) localFiles(dir string, pattern string) (map[uint64]uint64, []*FileInfo, error) {
	LOG.Debugf("list local files: addr[%v], dir[%v], pattern[%v]", w.addr, dir, pattern)

	meta := make(map[uint64]uint64)
	file, err := os.Open(path.Join(dir, MetaFile))
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
		if err := json.Unmarshal(bs, &meta); err != nil {
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

		if f.Name() == MetaFile {
			continue
		}

		if pattern != "" && !strings.Contains(f.Name(), pattern) {
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
		if list[i].Inode < list[j].Inode {
			return true
		}
		return false
	})

	return meta, list, nil
}

func (w *Worker) remoteFiles(dir, pattern string) ([]*FileInfo, error) {
	LOG.Debugf("list remote files: addr[%v], dir[%v], pattern[%v]", w.addr, dir, pattern)

	var list []*FileInfo

	req := RequestListFile{
		Dir:     dir,
		Pattern: pattern,
	}

	respData, err := Send(w.addr+PathListFile, &req)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(respData, &list); err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].Inode < list[j].Inode {
			return true
		}
		return false
	})

	return list, nil
}

func parseConfig(configPath string) {
	ipSyncMap = make(map[string]string)
	all, err := ioutil.ReadFile(configPath)
	if err != nil {
		panic(fmt.Sprintf("read %s has err:[%s]", configPath, err.Error()))
	}

	reg := regexp.MustCompile(`\s+`)
	for _, line := range strings.Split(string(all), "\n") {
		line = strings.TrimSpace(line)

		if len(line) == 0 {
			continue
		}

		if strings.TrimSpace(line)[0] == '#' {
			continue
		}

		split := reg.Split(line, -1)

		if _, found := workers[split[0]]; !found {
			workers[split[0]] = &Worker{
				addr: split[0],
			}
		}

		wk := workers[split[0]]

		wk.jobs = append(wk.jobs, &Job{
			src:     split[1],
			pattern: split[2],
			dist:    split[3],
		})

		if fi, err := os.Stat(split[3]); err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(split[3], os.ModePerm); err != nil {
					panic(err)
				}
				if err := os.MkdirAll(path.Join(split[3], "archive"), os.ModePerm); err != nil {
					panic(err)
				}
			} else {
				panic(err)
			}
		} else if !fi.IsDir() {
			panic(fmt.Sprintf("%s is not dir", split[3]))
		}

		ipSyncMap[split[3]] = split[0]

	}
}

func formatJsonLogMessage(opType int, inode uint64, src, dir, filename string, size int64) string {
	var (
		bytes []byte
		err   error
	)

	timeStr := time.Now().Format(TimeFormat)
	local, _ := time.LoadLocation("Local")
	localTime, _ := time.ParseInLocation(TimeFormat, timeStr, local)

	jsonLogItem := &LogItem{OpType: opType, Inode: inode, Src: src, Dir: dir, Filename: filename, Size: size, Time: localTime.Format(TimeFormat)}
	if bytes, err = json.Marshal(jsonLogItem); err != nil {
		LOG.Errorf("formatJsonLogMessage err: [%v]", err)
		return ""
	}
	return string(bytes)
}
