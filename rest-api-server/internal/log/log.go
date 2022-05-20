package log

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

func init() {
	InitLogger(NewLogConfig())
}

type LogConfig struct {
	Level      string
	Filename   string
	MaxSizeMB  int
	MaxBackups int
	MaxAge     int
}

func NewLogConfig() LogConfig {
	return LogConfig{
		Level:      "info",
		Filename:   "",
		MaxSizeMB:  100,
		MaxBackups: 10,
		MaxAge:     30,
	}
}

var currentLevel log.Level

func InitLogger(logConfig LogConfig) {
	lvl, err := log.ParseLevel(logConfig.Level)
	if err != nil {
		log.Errorf("Invlid log level %s ", logConfig.Level)
		lvl = log.ErrorLevel
	}

	currentLevel = lvl

	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
		DisableQuote:  true,
	})

	// setup log cutting
	if logConfig.Filename != "" {
		log.SetOutput(&lumberjack.Logger{
			Filename:   logConfig.Filename,
			MaxSize:    logConfig.MaxSizeMB,
			MaxBackups: logConfig.MaxBackups,
			MaxAge:     logConfig.MaxAge,
		})
	} else {
		log.SetOutput(os.Stdout)
	}

	RegisterLogCallBack()
}

func Tracef(format string, v ...interface{}) {
	if currentLevel >= log.TraceLevel { //  avoid unnecessary processing of fmt.Sprintf
		log.Trace(fmt.Sprintf(format, v...))
	}
}

func Debugf(format string, v ...interface{}) {
	if currentLevel >= log.DebugLevel {
		log.Debug(fmt.Sprintf(format, v...))
	}
}

func Infof(format string, v ...interface{}) {
	if currentLevel >= log.InfoLevel {
		log.Info(fmt.Sprintf(format, v...))
	}
}

func Warnf(format string, v ...interface{}) {
	if currentLevel >= log.WarnLevel {
		log.Warn(fmt.Sprintf(format, v...))
	}
}

func Errorf(format string, v ...interface{}) {
	if currentLevel >= log.ErrorLevel {
		log.Error(fmt.Sprintf(format, v...))
	}
}

func Fatalf(format string, v ...interface{}) {
	if currentLevel >= log.FatalLevel {
		log.Fatal(fmt.Sprintf(format, v...))
	}
}

func Panicf(format string, v ...interface{}) {
	if currentLevel >= log.PanicLevel {
		log.Panic(fmt.Sprintf(format, v...))
	}
}

func Trace(msg string) {
	if currentLevel >= log.TraceLevel {
		log.Trace(msg)
	}
}

func Debug(msg string) {
	if currentLevel >= log.DebugLevel {
		log.Debug(msg)
	}
}

func Info(msg string) {
	if currentLevel >= log.InfoLevel {
		log.Info(msg)
	}
}

func Warn(msg string) {
	if currentLevel >= log.WarnLevel {
		log.Warn(msg)
	}
}

func Error(msg string) {
	if currentLevel >= log.ErrorLevel {
		log.Error(msg)
	}
}

func Fatal(msg string) {
	if currentLevel >= log.FatalLevel {
		log.Fatal(msg)
	}
}

func Panic(msg string) {
	if currentLevel >= log.PanicLevel {
		log.Panic(msg)
	}
}

func IsTrace() bool {
	return currentLevel == log.TraceLevel
}

func IsDebug() bool {
	return currentLevel == log.DebugLevel
}

func IsInfo() bool {
	return currentLevel == log.InfoLevel
}

func IsWarn() bool {
	return currentLevel == log.WarnLevel
}

func IsError() bool {
	return currentLevel == log.ErrorLevel
}

func IsFatal() bool {
	return currentLevel == log.FatalLevel
}

func IsPanic() bool {
	return currentLevel == log.PanicLevel
}
