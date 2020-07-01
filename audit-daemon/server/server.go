package server

import (
	"encoding/json"
	. "github.com/chubaofs/chubaofs-tools/audit-daemon/util"
	"github.com/spf13/cast"
	"net/http"
	"sync"
)

func StartServer(port int) {
	mux := http.NewServeMux()
	mux.HandleFunc(PathForwardCmd, forwardCmdReq)

	server := &http.Server{
		Addr:    ":" + cast.ToString(port),
		Handler: mux,
	}
	LOG.Debugf("start server on")
	LOG.Fatal(server.ListenAndServe())
}

func forwardCmdReq(w http.ResponseWriter, r *http.Request) {
	var (
		req RequestForwardCmdReq
		err error
	)
	if err = ReadReq(r, &req); err != nil {
		SendErr(w, err)
		return
	}
	cmdReq := RequestCommand{
		Command: req.Command,
		LimitMB: req.LimitMB,
	}
	results := make([]string, len(req.AddrList))
	var wg sync.WaitGroup
	wg.Add(len(req.AddrList))
	for i, addr := range req.AddrList {
		go func(i int, addr string) {
			respData, err := Send(addr+PathCommand, &cmdReq)
			if err != nil {
				LOG.Errorf("forward cmd req err: addr[%v], req[%v]", addr, cmdReq)
				results[i] = "execute failed!"
				wg.Done()
				return
			}
			results[i] = string(respData)
			wg.Done()
		}(i, addr)
	}
	wg.Wait()

	resp, _ := json.Marshal(&ForwardCmdResponse{
		Code:    0,
		Msg:     "execute successfully",
		Results: results,
	})

	if _, err = w.Write(resp); err != nil {
		LOG.Errorf("write to server has err:[%s]", err.Error())
		SendErr(w, err)
		return
	}
}
