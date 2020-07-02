package daemon

import (
	"bufio"
	"encoding/json"
	"fmt"
	. "github.com/chubaofs/chubaofs-tools/audit-daemon/util"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cast"
)

func StartServer(port int) {
	mux := http.NewServeMux()
	mux.HandleFunc(PathReadFile, readFile)
	mux.HandleFunc(PathListFile, listDir)
	mux.HandleFunc(PathCommand, execCommand)

	// todo implement
	//go func() {
	//	monitorJob()
	//}()

	server := &http.Server{
		Addr:    ":" + cast.ToString(port),
		Handler: mux,
	}
	LOG.Debugf("start daemon server on :%d", port)
	LOG.Fatal(server.ListenAndServe())
}

func monitorJob() {
	for {
		time.Sleep(time.Second * 3)

		if err := os.MkdirAll("log/monitor", os.ModePerm); err != nil {
			panic(err)
		}

		state, err := monitor()
		if err != nil {
			LOG.Errorf("monitor has err :[%s]", err.Error())
			continue
		}

		bs, err := json.Marshal(state)
		if err != nil {
			LOG.Errorf("marshal has err :[%s]", err.Error())
			continue
		}

		filePath := "log/monitor/" + time.Now().Format("20060102") + ".log"

		var file *os.File
		_, err = os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				if file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, os.ModePerm); err != nil {
					LOG.Errorf("open file has err :[%s]", err.Error())
					continue
				}

			} else {
				LOG.Errorf("open file has err :[%s]", err.Error())
				continue
			}
		} else {
			if file, err = os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, os.ModePerm); err != nil {
				LOG.Errorf("open file has err :[%s]", err.Error())
				continue
			}
		}
		if err != nil {
			LOG.Errorf("open file has err :[%s]", err.Error())
			continue
		}
		if _, err = file.WriteString(string(bs) + "\n"); err != nil {
			LOG.Errorf("marshal has err :[%s]", err.Error())
			continue
		}
		_ = file.Close()
	}
}

func monitor() (*MachineState, error) {

	ms := &MachineState{
		Time:   time.Now(),
		Cpu:    100,
		Memory: 100,
		Ip:     "127.0.0.1",
	}

	return ms, nil
}

func listDir(w http.ResponseWriter, r *http.Request) {
	var req RequestListFile
	if err := ReadReq(r, &req); err != nil {
		SendErr(w, err)
		return
	}

	list, err := listFile(req.Dir, req.Pattern)
	if err != nil {
		SendErr(w, err)
		return
	}

	marshal, err := json.Marshal(list)
	if err != nil {
		SendErr(w, err)
		return
	}
	resp, _ := json.Marshal(&Response{
		Code: 0,
		Data: marshal,
	})

	if _, err := w.Write(resp); err != nil {
		LOG.Errorf("write to server has err:[%s]", err.Error())
	}
}

func readFile(w http.ResponseWriter, r *http.Request) {
	var req RequestGetContent
	if err := ReadReq(r, &req); err != nil {
		SendErr(w, err)
		return
	}

	content, err := getFileContent(&req)
	if err != nil {
		SendErr(w, err)
		return
	}

	resp, _ := json.Marshal(&Response{
		Code: 0,
		Data: content,
	})

	if _, err := w.Write(resp); err != nil {
		LOG.Errorf("write to server has err:[%s]", err.Error())
	}
}

func execCommand(w http.ResponseWriter, r *http.Request) {
	var (
		req    RequestCommand
		stdout io.ReadCloser
		err    error
	)
	if err = ReadReq(r, &req); err != nil {
		SendErr(w, err)
		return
	}
	cmd := exec.Command("/bin/bash", "-c", req.Command)
	cmd.Dir = req.Dir
	if stdout, err = cmd.StdoutPipe(); err != nil {
		SendErr(w, err)
		return
	}
	if err = cmd.Start(); err != nil {
		SendErr(w, err)
		return
	}
	reader := bufio.NewReader(stdout)
	result := make([]byte, 0)
	for {
		line, err2 := reader.ReadBytes('\n')
		if err2 != nil || io.EOF == err2 || len(result) >= req.LimitMB*1024*1024 {
			break
		}
		result = append(result, line...)
	}
	cmd.Wait()
	resp, _ := json.Marshal(&Response{
		Code: 0,
		Msg:  "execute successfully",
		Data: result,
	})

	if _, err = w.Write(resp); err != nil {
		LOG.Errorf("write to server has err:[%s]", err.Error())
		SendErr(w, err)
		return
	}
}

func getFileContent(req *RequestGetContent) ([]byte, error) {
	file, err := os.Open(path.Join(req.Dir, req.Name))
	if err != nil {
		LOG.Errorf("get file content: open err[%s]", err.Error())
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	f, err := file.Stat()
	if err != nil {
		LOG.Errorf("get file content: get file info err[%s]", err.Error())
		return nil, err
	}
	stat, ok := f.Sys().(*syscall.Stat_t)
	if !ok {
		LOG.Warningf("get file content: stat err")
		return nil, fmt.Errorf("not found inode by sys file:[%s]", file.Name())
	}

	if stat.Ino == req.Inode {
		return readFileContent(req, file)
	}

	infos, err := listFile(req.Dir, req.Pattern)
	if err != nil {
		return nil, err
	}

	for _, f := range infos {
		if f.Inode == req.Inode {
			file, err := os.Open(path.Join(req.Dir, f.Name))
			if err != nil {
				LOG.Errorf("get file content: open file[%s] err[%s]", f.Name, err.Error())
				return nil, err
			}
			defer func() {
				_ = file.Close()
			}()
			return readFileContent(req, file)
		}
	}

	return make([]byte, 0), nil

}

func readFileContent(req *RequestGetContent, file *os.File) ([]byte, error) {
	if req.Start > 0 {
		if _, err := file.Seek(req.Start, io.SeekStart); err != nil {
			LOG.Errorf("read file content: seek err[%s]", err.Error())
			return nil, err
		}
	}
	bytes := make([]byte, BodySize)
	read, err := file.Read(bytes)
	if err != nil && err != io.EOF {
		LOG.Errorf("get file content: read err[%s]", err.Error())
		return nil, err
	}
	return bytes[:read], nil
}

func listFile(dir, pattern string) ([]*FileInfo, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		LOG.Errorf("list files: read dir err[%s]", err.Error())
		return nil, err
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
	return list, nil
}
