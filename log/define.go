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

// Package log 日志结构定义
package log

import (
	"encoding/json"
	"fmt"

	"github.com/liangdas/mqant/mqtools"
)

// TraceSpan A SpanID refers to a single span.
type TraceSpan interface {

	// Trace is the root ID of the tree that contains all of the spans
	// related to this one.
	TraceID() string

	// Span is an ID that probabilistically uniquely identifies this
	// span.
	SpanID() string

	// 生产子TraceSpan
	ExtractSpan() TraceSpan

	// mqrpc.Marshaler
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	String() string
}

// TraceSpanImp TraceSpanImp
type TraceSpanImp struct {
	Trace string `json:"Trace"`
	Span  string `json:"Span"`
}

// TraceID TraceID
func (t *TraceSpanImp) TraceID() string {
	return t.Trace
}

// SpanID SpanID
func (t *TraceSpanImp) SpanID() string {
	return t.Span
}

// ExtractSpan ExtractSpan
func (t *TraceSpanImp) ExtractSpan() TraceSpan {
	return &TraceSpanImp{
		Trace: t.Trace,
		Span:  mqtools.GenerateID().String(),
	}
}
func (t *TraceSpanImp) Marshal() ([]byte, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
func (t *TraceSpanImp) Unmarshal(bytes []byte) error {
	return json.Unmarshal(bytes, t)
}
func (t *TraceSpanImp) String() string {
	return fmt.Sprintf("[%s] [%s]", t.Trace, t.Span)
}
