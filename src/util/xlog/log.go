/**
 * @File: log.go
 * @Author: 数据体系-xiesenxin
 * @Date：2023/5/3
 */
package xlog

import (
	"fmt"
	"runtime"
)

type Level int

const (
	LevelDebug Level = 1
	LevelInfo  Level = 2
	LevelError Level = 3
	LevelFatal Level = 4
)

var levelStr = map[Level]string{
	LevelDebug: "DEBUG",
	LevelInfo:  "INFO",
	LevelError: "ERROR",
	LevelFatal: "FATAL",
}

const globalLevel = LevelDebug

func logPrint(level Level, format string, vs ...interface{}) {
	if level < globalLevel {
		return
	}
	_, file, line, _ := runtime.Caller(1)
	fmt.Println(fmt.Sprintf("%s\t%s:%d\t", levelStr[level], file, line) + fmt.Sprintf(format, vs))
}

func LogDebug(format string, vs ...interface{}) {
	logPrint(LevelDebug, format, vs)
}

func LogInfo(format string, vs ...interface{}) {
	logPrint(LevelInfo, format, vs)
}
func LogError(format string, vs ...interface{}) {
	logPrint(LevelError, format, vs)
}

func LogFatal(format string, vs ...interface{}) {
	logPrint(LevelFatal, format, vs)
}
