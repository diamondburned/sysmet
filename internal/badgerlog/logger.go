package badgerlog

import "log"

// Why Badger does not expose the defaultLog instance is fucking beyond me.
// Honestly. What the fuck?

type Level uint8

const (
	NoLogging Level = iota
	ErrorLevel
	WarningLevel
	InfoLevel
	DebugLevel
)

type Logger struct {
	*log.Logger
	level Level
}

func NewDefaultLogger() *Logger {
	return NewLogger(log.Default(), WarningLevel)
}

func NewLogger(log *log.Logger, level Level) *Logger {
	return &Logger{
		Logger: log,
		level:  level,
	}
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	if l.level >= ErrorLevel {
		l.Printf("badger: error: "+format, args...)
	}
}

func (l *Logger) Warningf(format string, args ...interface{}) {
	if l.level >= WarningLevel {
		l.Printf("badger: warning: "+format, args...)
	}
}

func (l *Logger) Infof(format string, args ...interface{}) {
	if l.level >= InfoLevel {
		l.Printf("badger: info: "+format, args...)
	}
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	if l.level >= DebugLevel {
		l.Printf("badger: debug: "+format, args...)
	}
}
