package mqtools

import (
	"fmt"
	"math/rand"
	"runtime"
)

// RandInt64 生成一个[min,max)的随机数
func RandInt64(min, max int64) int64 {
	if min >= max {
		return max
	}
	return rand.Int63n(max-min) + min
}

// catch panic
func Catch(panicRet interface{}) error {
	if panicRet == nil {
		return nil
	}
	buf := make([]byte, 512*3)
	size := runtime.Stack(buf, false)
	stack := string(buf[0:size])
	return fmt.Errorf("%v, stack:\n%v", panicRet, stack)
}
