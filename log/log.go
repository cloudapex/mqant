// Copyright 2014 mqant Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package log 日志初始化
package log

import (
	beegolog "github.com/liangdas/mqant/log/beego"
	"github.com/liangdas/mqant/mqtools"
)

var beego *beegolog.BeeLogger
var bi *beegolog.BeeLogger

// InitLog 初始化日志
func InitLog(debug bool, ProcessID string, Logdir string, settings map[string]interface{}, logFilePath func(logdir, prefix, processID, suffix string) string) {
	beego = NewBeegoLogger(debug, ProcessID, Logdir, settings, logFilePath)
}

// InitBI 初始化BI日志
func InitBI(debug bool, ProcessID string, Logdir string, settings map[string]interface{}, logFilePath func(logdir, prefix, processID, suffix string) string) {
	bi = NewBeegoLogger(debug, ProcessID, Logdir, settings, logFilePath)
}

// Init 初始化配置
func Init(cc ...Option) {
	opt := &Options{}
	for _, o := range cc {
		o(opt)
	}
	InitBI(opt.Debug, opt.ProcessID, opt.BiDir, opt.BiSetting, opt.BIFileName)
	InitLog(opt.Debug, opt.ProcessID, opt.LogDir, opt.LogSetting, opt.LogFileName)
}

// LogBeego LogBeego
func LogBeego() *beegolog.BeeLogger {
	if beego == nil {
		beego = beegolog.NewLogger()
	}
	return beego
}

// BiBeego BiBeego
func BiBeego() *beegolog.BeeLogger {
	return bi
}

// CreateRootTrace CreateRootTrace
func CreateRootTrace() TraceSpan {
	return &TraceSpanImp{
		Trace: mqtools.GenerateID().String(),
		Span:  mqtools.GenerateID().String(),
	}
}

// CreateTrace CreateTrace
func CreateTrace(trace, span string) TraceSpan {
	return &TraceSpanImp{
		Trace: trace,
		Span:  span,
	}
}

// BiReport BiReport
func BiReport(msg string) {
	//gLogger.doPrintf(debugLevel, printDebugLevel, format, a...)
	l := BiBeego()
	if l != nil {
		l.BiReport(msg)
	}
}

// Debug Debug
func Debug(format string, a ...interface{}) {
	//gLogger.doPrintf(debugLevel, printDebugLevel, format, a...)
	LogBeego().Debug(nil, format, a...)
}

// Info Info
func Info(format string, a ...interface{}) {
	//gLogger.doPrintf(releaseLevel, printReleaseLevel, format, a...)
	LogBeego().Info(nil, format, a...)
}

// Error Error
func Error(format string, a ...interface{}) {
	//gLogger.doPrintf(errorLevel, printErrorLevel, format, a...)
	LogBeego().Error(nil, format, a...)
}

// Warning Warning
func Warning(format string, a ...interface{}) {
	//gLogger.doPrintf(fatalLevel, printFatalLevel, format, a...)
	LogBeego().Warning(nil, format, a...)
}

// TDebug TDebug
func TDebug(span TraceSpan, format string, a ...interface{}) {
	if span != nil {
		LogBeego().Debug(
			&beegolog.BeegoTraceSpan{
				Trace: span.TraceID(),
				Span:  span.SpanID(),
			}, format, a...)
	} else {
		LogBeego().Debug(nil, format, a...)
	}
}

// TInfo TInfo
func TInfo(span TraceSpan, format string, a ...interface{}) {
	if span != nil {
		LogBeego().Info(
			&beegolog.BeegoTraceSpan{
				Trace: span.TraceID(),
				Span:  span.SpanID(),
			}, format, a...)
	} else {
		LogBeego().Info(nil, format, a...)
	}
}

// TError TError
func TError(span TraceSpan, format string, a ...interface{}) {
	if span != nil {
		LogBeego().Error(
			&beegolog.BeegoTraceSpan{
				Trace: span.TraceID(),
				Span:  span.SpanID(),
			}, format, a...)
	} else {
		LogBeego().Error(nil, format, a...)
	}
}

// TWarning TWarning
func TWarning(span TraceSpan, format string, a ...interface{}) {
	if span != nil {
		LogBeego().Warning(
			&beegolog.BeegoTraceSpan{
				Trace: span.TraceID(),
				Span:  span.SpanID(),
			}, format, a...)
	} else {
		LogBeego().Warning(nil, format, a...)
	}
}

// Close Close
func Close() {
	LogBeego().Close()
}
