package util

import (
	stdlog "log"

	"github.com/op/go-logging"
	"gopkg.in/natefinch/lumberjack.v2"
)

var LOG = logging.MustGetLogger("audit-daemon")

func ConfigLog(module string, logLevel string) {
	level, err := logging.LogLevel(logLevel)
	if err != nil {
		panic(err)
	}

	var format = logging.MustStringFormatter(
		`%{asctime} %{shortfunc} [%{level:.4s}] - %{message}`,
	)

	var infoLogBackend logging.Backend
	if module == "gather" {
		infoLogBackend = SetLogFileLevel("./log/synclog/log_info.log", logging.INFO, logging.DefaultFormatter)
	} else {
		infoLogBackend = SetLogFileLevel("./log/"+module+"/log_info.log", logging.INFO, logging.DefaultFormatter)
	}
	warnLogBackend := SetLogFileLevel("./log/"+module+"/log_warn.log", logging.WARNING, format)
	errorLogBackend := SetLogFileLevel("./log/"+module+"/log_error.log", logging.ERROR, format)
	if level == logging.DEBUG {
		debugLogBackend := SetLogFileLevel("./log/"+module+"/log_debug.log", logging.DEBUG, format)
		logging.SetBackend(infoLogBackend, debugLogBackend, warnLogBackend, errorLogBackend)
	} else {
		logging.SetBackend(infoLogBackend, warnLogBackend, errorLogBackend)
	}

}

func SetLogFileLevel(logFileName string, logLevel logging.Level, format logging.Formatter) logging.Backend {
	var flag int
	switch format {
	case logging.DefaultFormatter:
		flag = 0
	default:
		flag = stdlog.Lshortfile
	}
	fileBackend := logging.NewLogBackend(&lumberjack.Logger{
		Filename:   logFileName,
		MaxSize:    512, // megabytes
		MaxBackups: 5,
		MaxAge:     28,   //days
		Compress:   true, // disabled by default
	}, "", flag)
	fileBackendFormatter := logging.NewBackendFormatter(fileBackend, format)
	fileBackendLevel := logging.AddModuleLevel(fileBackendFormatter)
	fileBackendLevel.SetLevel(logLevel, "audit-daemon")
	return fileBackendLevel
}
