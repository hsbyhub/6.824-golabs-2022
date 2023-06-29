/**
 * @File: log.go
 * @Author: 数据体系-xiesenxin
 * @Date：2023/5/3
 */
package logs

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"
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

func print(level Level, format string, vs ...interface{}) {
	if level < globalLevel {
		return
	}
	_, file, line, _ := runtime.Caller(2)
	_, file = filepath.Split(file)
	fmt.Println(fmt.Sprintf("%s\t%s\t%s:%d\t", levelStr[level], time.Now().Format("2006-01-02 15:04:05.000"), file, line) + fmt.Sprintf(format, vs...))
}

func Debug(format string, vs ...interface{}) {
	print(LevelDebug, format, vs...)
}

func Info(format string, vs ...interface{}) {
	print(LevelInfo, format, vs...)
}
func Error(format string, vs ...interface{}) {
	print(LevelError, format, vs...)
}

func Fatal(format string, vs ...interface{}) {
	print(LevelFatal, format, vs...)
}
