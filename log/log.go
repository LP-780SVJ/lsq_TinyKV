// High level log wrapper, so it can output different log based on level.
//
// There are five levels in total: FATAL, ERROR, WARNING, INFO, DEBUG.
// The default log output level is INFO, you can change it by:
// - call log.SetLevel()
// - set environment variable `LOG_LEVEL`

package log

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	Ldate         = log.Ldate
	Llongfile     = log.Llongfile
	Lmicroseconds = log.Lmicroseconds
	Lshortfile    = log.Lshortfile
	LstdFlags     = log.LstdFlags
	Ltime         = log.Ltime
	// 终端输出日志格式设置
	LforceTerminalFlags = Ltime | Lmicroseconds | Llongfile
	// 文件输出日志格式设置
	LforceFileFlags = Ltime | Lmicroseconds | Llongfile
)

// 自定义的强制flag设置，避免不同地方对flag的设置
var forceFlags = 0

type (
	LogLevel int
	LogType  int
)

const (
	LOG_FATAL   = LogType(0x1)
	LOG_ERROR   = LogType(0x2)
	LOG_WARNING = LogType(0x4)
	LOG_INFO    = LogType(0x8)
	LOG_DEBUG   = LogType(0x10)
	// 附加颜色支持
	LOG_DIY1 = LogType(0x20)
	LOG_DIY2 = LogType(0x40)
	LOG_DIY3 = LogType(0x80)
	LOG_DIY4 = LogType(0x100)
)

const (
	LOG_LEVEL_NONE  = LogLevel(0x0)
	LOG_LEVEL_FATAL = LOG_LEVEL_NONE | LogLevel(LOG_FATAL)
	LOG_LEVEL_ERROR = LOG_LEVEL_FATAL | LogLevel(LOG_ERROR)
	LOG_LEVEL_WARN  = LOG_LEVEL_ERROR | LogLevel(LOG_WARNING)
	LOG_LEVEL_INFO  = LOG_LEVEL_WARN | LogLevel(LOG_INFO)
	LOG_LEVEL_DEBUG = LOG_LEVEL_INFO | LogLevel(LOG_DEBUG)
	LOG_LEVEL_ALL   = LOG_LEVEL_DEBUG
)

var logTypeInfo = map[LogType]struct {
	name  string
	color string
}{
	LOG_FATAL:   {"fatal", "[1;31"},   // 粗体红
	LOG_ERROR:   {"error", "[0;31"},   // 红
	LOG_WARNING: {"warning", "[0;33"}, // 黄
	LOG_INFO:    {"info", "[0;32"},    // 绿
	LOG_DEBUG:   {"debug", "[0;36"},   // 青

	// 新增 DIY 类型
	LOG_DIY1: {"diy1", "[1;35"}, // 亮紫
	LOG_DIY2: {"diy2", "[1;34"}, // 亮蓝
	LOG_DIY3: {"diy3", "[1;36"}, // 亮青
	LOG_DIY4: {"diy4", "[1;33"}, // 亮黄
}

const FORMAT_TIME_DAY string = "20060102"
const FORMAT_TIME_HOUR string = "2006010215"

var _log *Logger = New()

func init() {
	SetFlags(Ldate | Ltime | Lshortfile)
	// 这里设置了非windows则强制染色，应该去掉
	// SetHighlighting(runtime.GOOS != "windows")
}

func GlobalLogger() *log.Logger {
	return _log._log
}

func SetLevel(level LogLevel) {
	_log.SetLevel(level)
}
func GetLogLevel() LogLevel {
	return _log.level
}

func SetFlags(flags int) {
	// 优先按照forceFlags设置
	if forceFlags != 0 {
		_log._log.SetFlags(forceFlags)
		return
	}
	_log._log.SetFlags(flags)
}

func Info(v ...interface{}) {
	_log.Info(v...)
}

func Infof(format string, v ...interface{}) {
	_log.Infof(format, v...)
}

func Panic(v ...interface{}) {
	_log.Panic(v...)
}

func Panicf(format string, v ...interface{}) {
	_log.Panicf(format, v...)
}

func Debug(v ...interface{}) {
	_log.Debug(v...)
}

func Debugf(format string, v ...interface{}) {
	_log.Debugf(format, v...)
}

func Warn(v ...interface{}) {
	_log.Warning(v...)
}

func Warnf(format string, v ...interface{}) {
	_log.Warningf(format, v...)
}

func Warning(v ...interface{}) {
	_log.Warning(v...)
}

func Warningf(format string, v ...interface{}) {
	_log.Warningf(format, v...)
}

func Error(v ...interface{}) {
	_log.Error(v...)
}

func Errorf(format string, v ...interface{}) {
	_log.Errorf(format, v...)
}

func Fatal(v ...interface{}) {
	_log.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	_log.Fatalf(format, v...)
}

func SetLevelByString(level string) {
	_log.SetLevelByString(level)
}

func SetHighlighting(highlighting bool) {
	_log.SetHighlighting(highlighting)
}

type Logger struct {
	_log         *log.Logger
	level        LogLevel
	highlighting bool
}

func (l *Logger) SetHighlighting(highlighting bool) {
	l.highlighting = highlighting
}

func (l *Logger) SetFlags(flags int) {
	l._log.SetFlags(flags)
}

func (l *Logger) Flags() int {
	return l._log.Flags()
}

func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

func (l *Logger) SetLevelByString(level string) {
	l.level = StringToLogLevel(level)
}

func (l *Logger) log(t LogType, v ...interface{}) {
	l.logf(t, "%v\n", v)
}

func (l *Logger) logf(t LogType, format string, v ...interface{}) {
	if l.level|LogLevel(t) != l.level {
		return
	}

	logStr, logColor := LogTypeToString(t)
	var s string
	if l.highlighting {
		s = "\033" + logColor + "m[" + logStr + "] " + fmt.Sprintf(format, v...) + "\033[0m"
	} else {
		s = "[" + logStr + "] " + fmt.Sprintf(format, v...)
	}
	l._log.Output(4, s)
}

// DIY日志颜色输出
func (l *Logger) DIYf(t LogType, tag string, format string, v ...interface{}) {

	_, logColor := LogTypeToString(t)
	var s string
	if l.highlighting {
		s = "\033" + logColor + "m[" + tag + "] " + fmt.Sprintf(format, v...) + "\033[0m"
	} else {
		s = "[" + tag + "] " + fmt.Sprintf(format, v...)
	}
	l._log.Output(3, s)
}

// 自定义日志颜色输出，外部接口
func DIYf(t LogType, tag string, format string, v ...interface{}) {
	_log.DIYf(t, tag, format, v...)
}

func (l *Logger) Fatal(v ...interface{}) {
	l.log(LOG_FATAL, v...)
	os.Exit(-1)
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.logf(LOG_FATAL, format, v...)
	os.Exit(-1)
}

func (l *Logger) Panic(v ...interface{}) {
	l._log.Panic(v...)
}

func (l *Logger) Panicf(format string, v ...interface{}) {
	l._log.Panicf(format, v...)
}

func (l *Logger) Error(v ...interface{}) {
	l.log(LOG_ERROR, v...)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logf(LOG_ERROR, format, v...)
}

func (l *Logger) Warning(v ...interface{}) {
	l.log(LOG_WARNING, v...)
}

func (l *Logger) Warningf(format string, v ...interface{}) {
	l.logf(LOG_WARNING, format, v...)
}

func (l *Logger) Debug(v ...interface{}) {
	l.log(LOG_DEBUG, v...)
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.logf(LOG_DEBUG, format, v...)
}

func (l *Logger) Info(v ...interface{}) {
	l.log(LOG_INFO, v...)
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.logf(LOG_INFO, format, v...)
}

func StringToLogLevel(level string) LogLevel {
	level = strings.ToLower(level)
	switch level {
	case "fatal":
		return LOG_LEVEL_FATAL
	case "error":
		return LOG_LEVEL_ERROR
	case "warn":
		return LOG_LEVEL_WARN
	case "warning":
		return LOG_LEVEL_WARN
	case "debug":
		return LOG_LEVEL_DEBUG
	case "info":
		return LOG_LEVEL_INFO
	}
	return LOG_LEVEL_ALL
}

func LogTypeToString(t LogType) (string, string) {
	if info, ok := logTypeInfo[t]; ok {
		return info.name, info.color
	}
	return "unknown", "[0;37" // 默认白色
}

func New() *Logger {
	return NewLogger(os.Stderr, "")
}

func NewLogger(w io.Writer, prefix string) *Logger {

	var highlighting = true
	forceFlags = LforceTerminalFlags

	// 如果设置了LOG_FILE，则修改输出目标为文件
	if path := os.Getenv("LOG_FILE"); len(path) != 0 {
		fd, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			panic("Failed to open file ")
		}
		w = fd
		highlighting = false
		forceFlags = LforceFileFlags

		var absPath string
		if absPath, err = filepath.Abs(path); err != nil {
			panic("Failed to get absolute path ")
		}
		fmt.Fprintf(os.Stderr, "\033[1;33m[log] writing log output to file: %s\033[0m\n", absPath)
	}
	// 分日志等级输出
	var level LogLevel
	if l := os.Getenv("LOG_LEVEL"); len(l) != 0 {
		level = StringToLogLevel(os.Getenv("LOG_LEVEL"))
	} else {
		level = LOG_LEVEL_INFO
	}
	return &Logger{_log: log.New(w, prefix, forceFlags), level: level, highlighting: highlighting}
}
