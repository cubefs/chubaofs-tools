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

package main

import (
	"fmt"
	"os"

	"github.com/chubaofs/chubaofs-tools/fault-inject/cmd"
	command "github.com/chubaofs/chubaofs/cli/cmd"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/spf13/cobra"
)

func runCLI() (err error) {
	var cfg *command.Config
	if cfg, err = command.LoadConfig(); err != nil {
		return
	}
	c := setupCommands(cfg)
	err = c.Execute()
	return
}

func setupCommands(cfg *command.Config) *cobra.Command {
	var mc = master.NewMasterClient(cfg.MasterAddr, false)
	return cmd.NewRootCmd(mc)
}

func main() {
	var err error
	if err = runCLI(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
