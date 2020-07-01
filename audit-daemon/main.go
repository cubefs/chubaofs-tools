package main

import (
	"flag"
	"fmt"
	"github.com/chubaofs/chubaofs-tools/audit-daemon/daemon"
	"github.com/chubaofs/chubaofs-tools/audit-daemon/gather"
	"github.com/chubaofs/chubaofs-tools/audit-daemon/server"
	"github.com/chubaofs/chubaofs-tools/audit-daemon/util"
)

var (
	model    = flag.String("model", "", "start model about 'gather', 'daemon' , 'server'")
	port     = flag.Int("port", 8080, "Port Settings for the service")
	logLevel = flag.String("log_level", "debug", "log level")

	//gather need config
	config = flag.String("gather_conf", "", "gather model config path")
	logDir = flag.String("gather_log", "", "gather model log dir for raft parse")
	dbAddr = flag.String("gather_db", "", "chubaodb address to send, eg: ip_address/put/table_name")
)

func main() {
	flag.Parse()

	util.ConfigLog(*model, *logLevel)

	switch *model {
	case "gather":
		if *config == "" {
			panic("must set -gather_conf in gather model")
		}

		if *logDir == "" {
			panic("must set -gather_log in gather model")
		}

		if *dbAddr == "" {
			panic("must set -gather_db in gather model")
		}

		gather.StartGather(*config)
		util.LOG.Fatal(gather.StartRaftParse(*logDir, *dbAddr))
	case "daemon":
		daemon.StartServer(*port)
	case "server":
		server.StartServer(*port)
	default:
		fmt.Println(fmt.Sprintf(" model type has err:[ not support `%s`] use 'audit-daemon -h' see more", *model))
	}
}
