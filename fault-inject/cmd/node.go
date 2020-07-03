// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	clicmd "github.com/chubaofs/chubaofs/cli/cmd"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

var (
	optNodeType   string
	optCommand    string
	optRemotePort string

	optTimeInterval int
)

func newRunCmd(config *clicmd.Config) *cobra.Command {
	var c = &cobra.Command{
		Use:   "run",
		Short: "iterate specified nodes and run command",
		Run: func(cmd *cobra.Command, args []string) {
			if optTimeInterval <= 0 {
				fmt.Printf("Invalid time interval: %v\n", optTimeInterval)
				os.Exit(1)
			}
			ti := time.Duration(optTimeInterval) * time.Second

			if optNodeType == "" || optRemotePort == "" {
				fmt.Printf("Missing mandatory options: node(%v) port(%v)\n", optNodeType, optRemotePort)
				os.Exit(1)
			}

			mc := master.NewMasterClient(config.MasterAddr, false)

			cv, err := mc.AdminAPI().GetCluster()
			if err != nil {
				fmt.Printf("Failed to get cluster from master: %v\n", err)
				os.Exit(1)
			}

			var nvs []proto.NodeView
			switch optNodeType {
			case "meta":
				nvs = cv.MetaNodes
			case "data":
				nvs = cv.DataNodes
			default:
				fmt.Printf("Unknown node type: %v\n", optNodeType)
				os.Exit(1)
			}

			for {
				for _, nv := range nvs {
					fmt.Printf("wait %v\n", ti)
					time.Sleep(ti)
					ip := strings.Split(nv.Addr, ":")[0]
					addr := fmt.Sprintf("%v:%v", ip, optRemotePort)
					runCmdOnRemote(addr, optCommand)
				}
			}
		},
	}

	c.Flags().StringVarP(&optNodeType, "node", "n", "", "Node on which the command is about to be executed")
	c.Flags().StringVarP(&optCommand, "command", "c", "", "Command to be executed on each node")
	c.Flags().StringVarP(&optRemotePort, "port", "p", "", "Remote daemon port")
	c.Flags().IntVarP(&optTimeInterval, "time", "t", 60, "Command execution time interval in SECONDS between nodes")
	return c
}

type RequestCommand struct {
	Dir     string
	Command string
	LimitMB int // the size of result
}

type Response struct {
	Code int32
	Msg  string
	Data []byte
}

func runCmdOnRemote(addr, command string) {
	fmt.Printf("Run command(%v) on node(%v) ... ", command, addr)

	url := fmt.Sprintf("http://%v/command", addr)
	client := http.Client{Timeout: 5 * time.Second}

	request := RequestCommand{Dir: "/", Command: command, LimitMB: 1}
	data, err := json.Marshal(request)
	if err != nil {
		fmt.Printf("fail(%v)\n", err)
		return
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		fmt.Printf("fail(%v)\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("fail(%v)\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("fail(%v)\n", resp.StatusCode)
		return
	}

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("fail(%v)\n", err)
		return
	}

	response := &Response{}
	err = json.Unmarshal(data, response)
	if err != nil {
		fmt.Printf("fail(%v)\n", err)
		return
	}

	fmt.Println()
	fmt.Println(string(response.Data))
	fmt.Printf("ok\n")
}
