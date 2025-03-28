package mqrpc

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	rpcpb "github.com/liangdas/mqant/mqrpc/pb"
	"github.com/liangdas/mqant/mqtools/uuid"
	"google.golang.org/protobuf/proto"
)

var functions = map[string]*FunctionInfo{}

func init() {
	registerFun("rpc", onRPCFunc)
	registerFun("rpc2", onRPCFunc2)
	registerFun("rpc3", onRPCFunc3)

}
func onRPCFunc(b bool, x int32, nn int64, f float32, ff float64, bt []byte, s string) (string, error) {
	fmt.Println("onRPCFunc 成功调用,请检查参数:", b, x, nn, f, ff, bt, s)
	return s, nil
}

type user struct {
	X int32
	N int64
	S string
}
type out struct {
	X int32
	N int64
	S string
}

func onRPCFunc2(u *user) (string, error) {
	fmt.Println("onRPCFunc2 成功调用,请检查参数:", u)
	return "ok", nil
}
func onRPCFunc3(u *user) (*out, error) {
	fmt.Println("onRPCFunc3 成功调用,请检查参数:", u)
	return nil, nil
}
func TestBytes(t *testing.T) {

	str, err := String(call(context.TODO(), "rpc", true, int32(100), int64(111), float32(1.1), 1.2222, []byte("0000"), "000"))
	t.Log("rpc", str, err)
	str, err = String(call(context.TODO(), "rpc2", &user{X: 1, N: 2, S: "str"}))
	t.Log("rpc2", str, err)
	ret := &out{}
	err = Gob(ret, RpcResult(call(context.TODO(), "rpc3", (*user)(nil) /*&user{X: 1, N: 2, S: "str"}*/)))
	t.Log("rpc3", ret, err)
}

// register
func registerFun(id string, f interface{}) {

	if _, ok := functions[id]; ok {
		panic(fmt.Sprintf("function id %v: already registered", id))
	}

	finfo := &FunctionInfo{
		Function:  reflect.ValueOf(f),
		FuncType:  reflect.ValueOf(f).Type(),
		Goroutine: true,
	}

	finfo.InType = []reflect.Type{}
	for i := 0; i < finfo.FuncType.NumIn(); i++ {
		rv := finfo.FuncType.In(i)
		finfo.InType = append(finfo.InType, rv)
	}
	functions[id] = finfo
}

// call
func call(ctx context.Context, _func string, params ...interface{}) (interface{}, error) {
	var ArgsType []string = make([]string, len(params))
	var args [][]byte = make([][]byte, len(params))
	for k, param := range params {
		var err error = nil
		ArgsType[k], args[k], err = Args2Bytes(nil, param)
		if err != nil {
			return nil, fmt.Errorf("args[%d] error %s", k, err.Error())
		}
	}

	caller, _ := os.Hostname()
	if ctx != nil {
		cr, ok := ctx.Value("caller").(string)
		if ok {
			caller = cr
		}
	}
	start := time.Now()
	var correlation_id = uuid.Rand().Hex()
	rpcInfo := &rpcpb.RPCInfo{
		Fn:       *proto.String(_func),
		Reply:    *proto.Bool(true),
		Expired:  *proto.Int64((start.UTC().Add(10 * time.Second).UnixNano()) / 1000000),
		Cid:      *proto.String(correlation_id),
		Args:     args,
		ArgsType: ArgsType,
		Caller:   *proto.String(caller),
		Hostname: *proto.String(caller),
	}

	callInfo := &CallInfo{
		RPCInfo: rpcInfo,
	}
	_runFunc(callInfo)

	result, err := Bytes2Args(nil, callInfo.Result.ResultType, callInfo.Result.Result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ...
func _runFunc(callInfo *CallInfo) {
	functionInfo, ok := functions[callInfo.RPCInfo.Fn]
	if !ok {
		panic(fmt.Sprintf("function id %v: not found", callInfo.RPCInfo.Fn))
	}

	f := functionInfo.Function
	fType := functionInfo.FuncType
	fInType := functionInfo.InType
	params := callInfo.RPCInfo.Args
	ArgsType := callInfo.RPCInfo.ArgsType
	if len(params) != fType.NumIn() {
		panic(fmt.Sprintf("The number of params %v is not adapted.%v", params, f.String()))
	}

	//t:=RandInt64(2,3)
	//time.Sleep(time.Second*time.Duration(t))
	// f 为函数地址
	var in []reflect.Value
	var input []interface{}
	if len(ArgsType) > 0 {
		in = make([]reflect.Value, len(params))
		input = make([]interface{}, len(params))
		for k, v := range ArgsType {
			rv := fInType[k]

			var isPtr = false
			var elemp reflect.Value
			if rv.Kind() == reflect.Ptr { // 如果是指针类型就得取到指针所代表的具体类型
				isPtr = true
				elemp = reflect.New(rv.Elem())
			} else {
				elemp = reflect.New(rv)
			}

			ret, err := Bytes2Args(nil, v, params[k])
			if err != nil {
				panic(err)
				return
			}

			switch {
			case strings.HasPrefix(v, MARSHAL):
				if err := Marshal(elemp.Interface(), RpcResult(ret, nil)); err != nil {
					panic(err)
					return
				}
				if isPtr {
					in[k] = reflect.ValueOf(elemp.Interface()) //接收 指针变量
				} else {
					in[k] = elemp.Elem() // 接收 值变量
				}
			case strings.HasPrefix(v, PBPROTO):
				if err := Proto(elemp.Interface(), RpcResult(ret, nil)); err != nil {
					panic(err)
					return
				}
				if isPtr {
					in[k] = reflect.ValueOf(elemp.Interface()) //接收 指针变量
				} else {
					in[k] = elemp.Elem() // 接收 值变量
				}
			case strings.HasPrefix(v, GOB):
				if err := Gob(elemp.Interface(), RpcResult(ret, nil)); err != nil {
					panic(err)
					return
				}
				if isPtr {
					in[k] = reflect.ValueOf(elemp.Interface()) //接收 指针变量
				} else {
					in[k] = elemp.Elem() // 接收 值变量
				}
			default: // 其他的当做值类型处理
				switch ret.(type) {
				case nil:
					in[k] = reflect.Zero(rv)
				default:
					in[k] = reflect.ValueOf(ret)
				}
			}
			input[k] = in[k].Interface()
		}
	}

	out := f.Call(in)
	var rs []interface{}
	if len(out) != 2 {
		panic(fmt.Sprintf("%s rpc func(%s) return error %s\n", "ModuleType", callInfo.RPCInfo.Fn, "func(....)(result interface{}, err error)"))
	}
	if len(out) > 0 { //prepare out paras
		rs = make([]interface{}, len(out), len(out))
		for i, v := range out {
			rs[i] = v.Interface()
		}
	}

	var rerr string
	switch e := rs[1].(type) {
	case string:
		rerr = e
		break
	case error:
		rerr = e.Error()
	case nil:
		rerr = ""
	default:
		panic(fmt.Sprintf("%s rpc func(%s) return error %s\n", "ModuleType", callInfo.RPCInfo.Fn, "func(....)(result interface{}, err error)"))
	}
	argsType, args, err := Args2Bytes(nil, rs[0])
	if err != nil {
		panic(err)
	}

	resultInfo := &rpcpb.ResultInfo{
		Cid:        callInfo.RPCInfo.Cid,
		Error:      rerr,
		ResultType: argsType,
		Result:     args,
	}
	callInfo.Result = resultInfo
}
