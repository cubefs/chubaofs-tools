package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

func DrawMap(maps map[string]interface{}, split string) map[string]interface{} {
	newMap := make(map[string]interface{})
	drawMap(newMap, maps, "", split)
	return newMap
}

func drawMap(result, maps map[string]interface{}, prefix, split string) {
	newPrefix := prefix
	for k, v := range maps {

		if prefix == "" {
			newPrefix = k
		} else {
			newPrefix = prefix + split + k
		}

		switch v.(type) {
		case map[string]interface{}:
			drawMap(result, v.(map[string]interface{}), newPrefix, split)
		default:
			result[newPrefix] = v
		}
	}
}

type Response struct {
	Code int32
	Msg  string
	Data []byte
}

func WriteMeta(metaPath string, meta interface{}) {
	bs, err := json.Marshal(meta)
	if err != nil {
		LOG.Errorf("marshal meta:[%s] has err:[%s]", metaPath, err.Error())
	}
	err = ioutil.WriteFile(metaPath, bs, os.ModePerm)
	if err != nil {
		LOG.Errorf("write meta:[%s] has err:[%s]", metaPath, err.Error())
	}
}

func Send(url string, request interface{}) ([]byte, error) {
	if url[:7] != "http://" {
		url = "http://" + url
	}

	body, err := json.Marshal(request)
	if err != nil {
		LOG.Errorf("send request[%s]: marshal err: [%s]", url, err.Error())
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		LOG.Errorf("send request[%s]: new request err: [%s]", url, err.Error())
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("authorization", DefPassword)

	do, err := http.DefaultClient.Do(req)
	if err != nil {
		LOG.Errorf("send request[%s]: do client err: [%s]", url, err.Error())
		return nil, err
	}

	if do.StatusCode != 200 {
		LOG.Warningf("send request[%s]: status code: [%v]", url, do.StatusCode)
		return nil, fmt.Errorf("post has status:[%d] err:[%s]", do.StatusCode, do.Body)
	}

	all, err := ioutil.ReadAll(do.Body)
	_ = do.Body.Close()
	if err != nil {
		LOG.Errorf("send request[%s]: read body err: [%s]", url, err.Error())
		return nil, err
	}

	var resp Response
	if err = json.Unmarshal(all, &resp); err != nil {
		LOG.Errorf("send request[%s]: unmarshal err: [%s]", url, err.Error())
		return nil, err
	}

	if resp.Code > 0 {
		LOG.Warningf("send request[%s]: response code: [%v]", url, resp.Code)
		return nil, fmt.Errorf(resp.Msg)
	}

	return resp.Data, nil
}

func ReadReq(r *http.Request, req interface{}) error {
	all, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(all, req)
}

func SendErr(w http.ResponseWriter, err error) {
	rep, _ := json.Marshal(&Response{
		Code: 1,
		Msg:  err.Error(),
	})
	if _, err := w.Write(rep); err != nil {
		LOG.Errorf("write to server has err:[%s]", err.Error())
	}
}
