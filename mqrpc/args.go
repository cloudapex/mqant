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

func Args2Bytes(serializes map[string]RPCSerialize, arg interface{}) (string, []byte, error) {
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

		for _, v := range serializes {
			ptype, vk, err := v.Serialize(arg)
			if err == nil {
				//解析成功了
				return ptype, vk, err
			}
		}

		if rv.IsNil() {
			//如果是nil则直接返回
			return NULL, nil, nil
		}

		if v2, ok := arg.(Marshaler); ok {
			b, err := v2.Marshal()
			if err != nil {
				return "", nil, fmt.Errorf("args [%s] marshal error %v", reflect.TypeOf(arg), err)
			}
			return fmt.Sprintf("%v@%v", MARSHAL, reflect.TypeOf(arg)), b, nil
		}
		if v2, ok := arg.(proto.Message); ok {
			b, err := proto.Marshal(v2)
			if err != nil {
				log.Error("proto.Marshal error")
				return "", nil, fmt.Errorf("args [%s] proto.Marshal error %v", reflect.TypeOf(arg), err)
			}
			return fmt.Sprintf("%v@%v", PBPROTO, reflect.TypeOf(arg)), b, nil
		}
		// 默认使用gob编码
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		if err := encoder.Encode(arg); err != nil {
			return "", nil, fmt.Errorf("args [%s] gob encode(default) error %v", reflect.TypeOf(arg), err)
		}
		return fmt.Sprintf("%v@%v", GOB, reflect.TypeOf(arg)), buf.Bytes(), nil
	}
}

func BytesToArgs(serializes map[string]RPCSerialize, argsType string, args []byte, inType reflect.Type) (reflect.Value, error) {
	var isPtr = false
	var elemp reflect.Value
	if inType.Kind() == reflect.Ptr { //如果是指针类型就得取到指针所代表的具体类型
		isPtr = true
		elemp = reflect.New(inType.Elem())
	} else {
		elemp = reflect.New(inType)
	}

	switch {
	case argsType == NULL:
		return elemp, nil
	case argsType == STRING:
		return reflect.ValueOf(string(args)), nil
	case argsType == BOOL:
		return reflect.ValueOf(mqtools.BytesToBool(args)), nil
	case argsType == INT:
		return reflect.ValueOf(mqtools.BytesToInt32(args)), nil
	case argsType == LONG:
		return reflect.ValueOf(mqtools.BytesToInt64(args)), nil
	case argsType == FLOAT:
		return reflect.ValueOf(mqtools.BytesToFloat32(args)), nil
	case argsType == DOUBLE:
		return reflect.ValueOf(mqtools.BytesToFloat64(args)), nil
	case argsType == BYTES:
		return reflect.ValueOf(args), nil
	case argsType == MAP:
		mps, err := mqtools.BytesToMap(args)
		if err != nil {
			return elemp, err
		}
		return reflect.ValueOf(mps), nil
	case argsType == MAPSTR:
		mps, err := mqtools.BytesToMapString(args)
		if err != nil {
			return elemp, err
		}
		return reflect.ValueOf(mps), nil
	case argsType == TRACE:
		trace := &log.TraceSpanImp{}
		err := json.Unmarshal(args, trace)
		if err != nil {
			return elemp, err
		}
		return reflect.ValueOf(trace.ExtractSpan()), nil
	case strings.HasPrefix(argsType, MARSHAL):
		mr, ok := elemp.Interface().(Marshaler)
		if !ok {
			return elemp, fmt.Errorf("args [%s] no implemented interface(marshal)", argsType)
		}
		if err := mr.Unmarshal(args); err != nil {
			return elemp, err
		}
		if isPtr {
			return reflect.ValueOf(mr), nil
		}
		return elemp.Elem(), nil
	case strings.HasPrefix(argsType, PBPROTO):
		pb, ok := elemp.Interface().(proto.Message)
		if !ok {
			return elemp, fmt.Errorf("args [%s] no implemented interface(proto.Message)", argsType)
		}
		if err := proto.Unmarshal(args, pb); err != nil {
			return elemp, err
		}
		if isPtr {
			return reflect.ValueOf(pb), nil
		}
		return elemp.Elem(), nil
	case strings.HasPrefix(argsType, GOB):
		er := elemp.Interface()
		decoder := gob.NewDecoder(bytes.NewBuffer(args))
		if err := decoder.Decode(elemp.Interface()); err != nil {
			return elemp, fmt.Errorf("args [%s] gob decode error: %v", argsType, err)
		}
		if isPtr {
			return reflect.ValueOf(er), nil
		}
		return elemp.Elem(), nil
	default:
		for _, v := range serializes {
			vk, err := v.Deserialize(argsType, args)
			if err == nil {
				return reflect.ValueOf(vk), nil
			}
		}
		return elemp, fmt.Errorf("Bytes2Args [%s] not registered to app.addrpcserialize(...)", argsType)
	}
}
func Bytes2Args(serializes map[string]RPCSerialize, argsType string, args []byte) (interface{}, error) {
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
		mps, errs := mqtools.BytesToMap(args)
		if errs != nil {
			return nil, errs
		}
		return mps, nil
	case argsType == MAPSTR:
		mps, errs := mqtools.BytesToMapString(args)
		if errs != nil {
			return nil, errs
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
	default:
		for _, v := range serializes {
			vk, err := v.Deserialize(argsType, args)
			if err == nil {
				//解析成功了
				return vk, err
			}
		}
		return nil, fmt.Errorf("Bytes2Args [%s] not registered to app.addrpcserialize(...)", argsType)
	}
}
