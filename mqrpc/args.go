// Copyright 2014 loolgame Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mqrpc

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/mqtools"
	"google.golang.org/protobuf/proto"
)

var (
	NULL    = "null"    //nil   null
	BOOL    = "bool"    //bool
	INT     = "int"     //int
	LONG    = "long"    //long64
	FLOAT   = "float"   //float32
	DOUBLE  = "double"  //float64
	BYTES   = "bytes"   //[]byte
	STRING  = "string"  //string
	MAP     = "map"     //map[string]interface{}
	MAPSTR  = "mapstr"  //map[string]string{}
	TRACE   = "trace"   //log.TraceSpanImp
	MARSHAL = "marshal" //mqrpc.Marshaler
	PBPROTO = "pbproto" //proto.Message
	GOB     = "gob"     //go gob(default struct)
)

func Args2Bytes(arg interface{}) (string, []byte, error) {
	if arg == nil {
		return NULL, nil, nil
	}

	switch v2 := arg.(type) {
	case string:
		return STRING, []byte(v2), nil
	case bool:
		return BOOL, mqtools.BoolToBytes(v2), nil
	case int32:
		return INT, mqtools.Int32ToBytes(v2), nil
	case int64:
		return LONG, mqtools.Int64ToBytes(v2), nil
	case float32:
		return FLOAT, mqtools.Float32ToBytes(v2), nil
	case float64:
		return DOUBLE, mqtools.Float64ToBytes(v2), nil
	case []byte:
		return BYTES, v2, nil
	case map[string]interface{}:
		bytes, err := mqtools.MapToBytes(v2)
		if err != nil {
			return MAP, nil, err
		}
		return MAP, bytes, nil
	case map[string]string:
		bytes, err := mqtools.MapToBytesString(v2)
		if err != nil {
			return MAPSTR, nil, err
		}
		return MAPSTR, bytes, nil
	case log.TraceSpanImp: // 这两个内置的结构
		bytes, err := json.Marshal(v2)
		if err != nil {
			return TRACE, nil, err
		}
		return TRACE, bytes, nil
	case *log.TraceSpanImp: // 这两个内置的结构
		bytes, err := json.Marshal(v2)
		if err != nil {
			return TRACE, nil, err
		}
		return TRACE, bytes, nil
	default:
		rv := reflect.ValueOf(arg)
		if rv.Kind() != reflect.Ptr {
			return "", nil, fmt.Errorf("Args2Bytes [%v] not pointer type", reflect.TypeOf(arg))
		}
		if rv.IsNil() {
			//如果是nil则直接返回
			return NULL, nil, nil
		}
		if rv.Elem().Kind() != reflect.Struct {
			return "", nil, fmt.Errorf("Args2Bytes [%v] not struct type", reflect.TypeOf(arg))
		}

		// use mqrpc.Marshaler
		if v2, ok := arg.(Marshaler); ok {
			b, err := v2.Marshal()
			if err != nil {
				return "", nil, fmt.Errorf("args [%s] marshal error %v", reflect.TypeOf(arg), err)
			}
			return fmt.Sprintf("%v@%v", MARSHAL, reflect.TypeOf(arg)), b, nil
		}
		// use proto.Message
		if v2, ok := arg.(proto.Message); ok {
			b, err := proto.Marshal(v2)
			if err != nil {
				log.Error("proto.Marshal error")
				return "", nil, fmt.Errorf("args [%s] proto.Marshal error %v", reflect.TypeOf(arg), err)
			}
			return fmt.Sprintf("%v@%v", PBPROTO, reflect.TypeOf(arg)), b, nil
		}
		// use gob coding (default)
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		if err := encoder.Encode(arg); err != nil {
			return "", nil, fmt.Errorf("args [%s] gob encode(default) error %v", reflect.TypeOf(arg), err)
		}
		return fmt.Sprintf("%v@%v", GOB, reflect.TypeOf(arg)), buf.Bytes(), nil
	}
}

func Bytes2Args(argsType string, args []byte) (interface{}, error) {
	switch {
	case argsType == NULL:
		return nil, nil
	case argsType == STRING:
		return string(args), nil
	case argsType == BOOL:
		return mqtools.BytesToBool(args), nil
	case argsType == INT:
		return mqtools.BytesToInt32(args), nil
	case argsType == LONG:
		return mqtools.BytesToInt64(args), nil
	case argsType == FLOAT:
		return mqtools.BytesToFloat32(args), nil
	case argsType == DOUBLE:
		return mqtools.BytesToFloat64(args), nil
	case argsType == BYTES:
		return args, nil
	case argsType == MAP:
		mps, err := mqtools.BytesToMap(args)
		if err != nil {
			return nil, err
		}
		return mps, nil
	case argsType == MAPSTR:
		mps, err := mqtools.BytesToMapString(args)
		if err != nil {
			return nil, err
		}
		return mps, nil
	case argsType == TRACE:
		trace := &log.TraceSpanImp{}
		err := json.Unmarshal(args, trace)
		if err != nil {
			return nil, err
		}
		return trace.ExtractSpan(), nil
	case strings.HasPrefix(argsType, MARSHAL): // 不能直接解出对象
		return args, nil
	case strings.HasPrefix(argsType, PBPROTO): // 不能直接解出对象
		return args, nil
	case strings.HasPrefix(argsType, GOB): // 不能直接解出对象
		return args, nil
	}
	return nil, fmt.Errorf("Bytes2Args [%s] unsupported argsType", argsType)
}
