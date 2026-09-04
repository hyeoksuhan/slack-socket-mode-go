package socketmode

import (
	"io"
	"log"
	"os"
	"strings"
)

// LogLevel selects how much the client says.
type LogLevel string

const (
	LogError LogLevel = "error"
	LogWarn  LogLevel = "warn"
	LogInfo  LogLevel = "info"
	LogDebug LogLevel = "debug"
)

func (l LogLevel) rank() int {
	switch strings.ToLower(string(l)) {
	case string(LogDebug):
		return 0
	case string(LogInfo):
		return 1
	case string(LogWarn):
		return 2
	case string(LogError):
		return 3
	default:
		return 1
	}
}

// Logger is the hook for sending client output somewhere other than stderr.
// Implement it to route into an existing structured logger.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// stdLogger is the default: the standard library, filtered by level.
type stdLogger struct {
	l     *log.Logger
	level LogLevel
}

// NewLogger returns a Logger writing to w at the given level.
// Pass io.Discard to silence the client entirely.
func NewLogger(w io.Writer, level LogLevel) Logger {
	if w == nil {
		w = os.Stderr
	}
	return &stdLogger{l: log.New(w, "socketmode ", log.LstdFlags), level: level}
}

func (s *stdLogger) emit(at LogLevel, format string, args ...any) {
	if at.rank() < s.level.rank() {
		return
	}
	s.l.Printf(strings.ToUpper(string(at))+" "+format, args...)
}

func (s *stdLogger) Debugf(f string, a ...any) { s.emit(LogDebug, f, a...) }
func (s *stdLogger) Infof(f string, a ...any)  { s.emit(LogInfo, f, a...) }
func (s *stdLogger) Warnf(f string, a ...any)  { s.emit(LogWarn, f, a...) }
func (s *stdLogger) Errorf(f string, a ...any) { s.emit(LogError, f, a...) }
