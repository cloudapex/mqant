// Copyright 2014 mqant Author. All Rights Reserved.
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
package gatebase

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/gate/base/mqtt"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqrpc"
	"github.com/liangdas/mqant/mqtools"
	"github.com/liangdas/mqant/network"
)

//type resultInfo struct {
//	Error  string      //错误结果 如果为nil表示请求正确
//	Result interface{} //rpc 返回结果
//}

type agent struct {
	gate.Agent
	module                       module.RPCModule
	session                      gate.Session
	conn                         network.Conn
	r                            *bufio.Reader
	w                            *bufio.Writer
	gate                         gate.Gate
	client                       *mqtt.Client
	ch                           chan int //控制模块可同时开启的最大协程数
	isclose                      bool
	protocol_ok                  bool
	lock                         sync.Mutex
	lastStorageHeartbeatDataTime time.Duration //上一次发送存储心跳时间
	revNum                       int64
	sendNum                      int64
	connTime                     time.Time
}

func NewMqttAgent(module module.RPCModule) *agent {
	a := &agent{
		module: module,
	}
	return a
}
func (this *agent) OnInit(gate gate.Gate, conn network.Conn) error {
	this.ch = make(chan int, gate.Options().ConcurrentTasks)
	this.conn = conn
	this.gate = gate
	this.r = bufio.NewReaderSize(conn, gate.Options().BufSize)
	this.w = bufio.NewWriterSize(conn, gate.Options().BufSize)
	this.isclose = false
	this.protocol_ok = false
	this.revNum = 0
	this.sendNum = 0
	this.lastStorageHeartbeatDataTime = time.Duration(time.Now().UnixNano())
	return nil
}
func (this *agent) IsClosed() bool {
	return this.isclose
}

func (this *agent) ProtocolOK() bool {
	return this.protocol_ok
}

func (this *agent) GetSession() gate.Session {
	return this.session
}

func (this *agent) Wait() error {
	// 如果ch满了则会处于阻塞，从而达到限制最大协程的功能
	select {
	case this.ch <- 1:
	//do nothing
	default:
		//warnning!
		return fmt.Errorf("the work queue is full!")
	}
	return nil
}
func (this *agent) Finish() {
	// 完成则从ch推出数据
	select {
	case <-this.ch:
	default:
	}
}

func (this *agent) Run() (err error) {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("conn.serve() panic(%v)\n info:%s", err, string(buff))
		}
		this.Close()

	}()
	go func() {
		defer func() {
			if err := recover(); err != nil {
				buff := make([]byte, 1024)
				runtime.Stack(buff, false)
				log.Error("OverTime panic(%v)\n info:%s", err, string(buff))
			}
		}()
		select {
		case <-time.After(this.gate.Options().OverTime):
			if this.GetSession() == nil {
				//超过一段时间还没有建立mqtt连接则直接关闭网络连接
				this.Close()
			}

		}
	}()

	//握手协议
	var pack *mqtt.Pack
	pack, err = mqtt.ReadPack(this.r, this.gate.Options().MaxPackSize)
	if err != nil {
		log.Error("Read login pack error %v", err)
		return
	}
	if pack.GetType() != mqtt.CONNECT {
		log.Error("Recive login pack's type error:%v \n", pack.GetType())
		return
	}
	conn, ok := (pack.GetVariable()).(*mqtt.Connect)
	if !ok {
		log.Error("It's not this mqtt connection package.")
		return
	}
	//id := info.GetUserName()
	//psw := info.GetPassword()
	//log.Debug("Read login pack %s %s %s %s",*id,*psw,info.GetProtocol(),info.GetVersion())
	c := mqtt.NewClient(this.module.GetApp().Configs().Mqtt, this, this.r, this.w, this.conn, conn.GetKeepAlive(), this.gate.Options().MaxPackSize)
	this.client = c
	addr := this.conn.RemoteAddr()
	this.session, err = NewSessionByMap(this.module.GetApp(), map[string]interface{}{
		"Sessionid": mqtools.GenerateID().String(),
		"Network":   addr.Network(),
		"IP":        addr.String(),
		"Serverid":  this.module.GetServerID(),
		"Settings":  make(map[string]string),
	})
	netConn, ok := this.conn.(*network.WSConn)
	if ok {
		//如果是websocket连接 提取 User-Agent
		this.session.SetLocalKV("User-Agent", netConn.Conn().Request().Header.Get("User-Agent"))
	}
	if err != nil {
		log.Error("gate create agent fail", err.Error())
		return
	}
	this.session.SetGuestJudger(this.gate.GetGuestJudger())
	this.session.CreateTrace() //代码跟踪
	//回复客户端 CONNECT
	err = mqtt.WritePack(mqtt.GetConnAckPack(0), this.w)
	if err != nil {
		log.Error("ConnAckPack error %v", err.Error())
		return
	}
	this.connTime = time.Now()
	this.protocol_ok = true
	this.gate.GetAgentLearner().Connect(this) //发送连接成功的事件
	c.Listen_loop()                           //开始监听,直到连接中断
	return nil
}

func (this *agent) OnClose() error {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("agent OnClose panic(%v)\n info:%s", err, string(buff))
		}
	}()
	this.isclose = true
	this.gate.GetAgentLearner().DisConnect(this) //发送连接断开的事件
	return nil
}

func (this *agent) GetError() error {
	return this.client.GetError()
}

func (this *agent) RevNum() int64 {
	return this.revNum
}
func (this *agent) SendNum() int64 {
	return this.sendNum
}
func (this *agent) ConnTime() time.Time {
	return this.connTime
}
func (this *agent) OnRecover(pack *mqtt.Pack) {
	err := this.Wait()
	if err != nil {
		log.Error("Gate OnRecover error [%v]", err)
		pub := pack.GetVariable().(*mqtt.Publish)
		this.toResult(this, *pub.GetTopic(), nil, err.Error())
	} else {
		go this.recoverworker(pack)
	}
}

func (this *agent) toResult(a *agent, Topic string, Result interface{}, Error string) error {
	switch v2 := Result.(type) {
	case module.ProtocolMarshal:
		return a.WriteMsg(Topic, v2.GetData())
	}
	b, err := a.module.GetApp().ProtocolMarshal(a.session.TraceID(), Result, Error)
	if err == "" {
		if b != nil {
			return a.WriteMsg(Topic, b.GetData())
		}
		return nil
	}
	br, _ := a.module.GetApp().ProtocolMarshal(a.session.TraceID(), nil, err)
	return a.WriteMsg(Topic, br.GetData())
}

func (this *agent) recoverworker(pack *mqtt.Pack) {
	defer func() {
		this.lock.Lock()
		interval := int64(this.lastStorageHeartbeatDataTime) + int64(this.gate.Options().Heartbeat) //单位纳秒
		this.lock.Unlock()
		if interval < time.Now().UnixNano() {
			if this.gate.GetStorageHandler() != nil {
				this.lock.Lock()
				this.lastStorageHeartbeatDataTime = time.Duration(time.Now().UnixNano())
				this.lock.Unlock()
				this.gate.GetStorageHandler().Heartbeat(this.GetSession())
			}
		}
		this.Finish()
		if r := recover(); r != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("Gate recoverworker error [%v] stack : %v", r, string(buff))
		}
	}()

	toResult := this.toResult
	//路由服务
	switch pack.GetType() {
	case mqtt.PUBLISH:
		this.lock.Lock()
		this.revNum = this.revNum + 1
		this.lock.Unlock()
		pub := pack.GetVariable().(*mqtt.Publish)
		if this.gate.GetRouteHandler() != nil {
			needreturn, result, err := this.gate.GetRouteHandler().OnRoute(this.GetSession(), *pub.GetTopic(), pub.GetMsg())
			if err != nil {
				if needreturn {
					toResult(this, *pub.GetTopic(), result, err.Error())
				}
				return
			}
			if needreturn {
				toResult(this, *pub.GetTopic(), result, "")
			}
		} else {
			topics := strings.Split(*pub.GetTopic(), "/")
			var msgid string
			if len(topics) < 2 {
				errorstr := "Topic must be [moduleType@moduleID]/[handler]|[moduleType@moduleID]/[handler]/[msgid]"
				log.Error(errorstr)
				toResult(this, *pub.GetTopic(), nil, errorstr)
				return
			} else if len(topics) == 3 {
				msgid = topics[2]
			}
			startsWith := strings.HasPrefix(topics[1], "HD_")
			if !startsWith {
				if msgid != "" {
					toResult(this, *pub.GetTopic(), nil, fmt.Sprintf("Method(%s) must begin with 'HD_'", topics[1]))
				}
				return
			}
			var argsType []string = make([]string, 2)
			var args [][]byte = make([][]byte, 2)
			serverSession, err := this.module.GetRouteServer(topics[0])
			if err != nil {
				if msgid != "" {
					toResult(this, *pub.GetTopic(), nil, fmt.Sprintf("Service(type:%s) not found", topics[0]))
				}
				return
			}
			if len(pub.GetMsg()) > 0 && pub.GetMsg()[0] == '{' && pub.GetMsg()[len(pub.GetMsg())-1] == '}' {
				//尝试解析为json为map
				var obj interface{} // var obj map[string]interface{}
				err := json.Unmarshal(pub.GetMsg(), &obj)
				if err != nil {
					if msgid != "" {
						toResult(this, *pub.GetTopic(), nil, "The JSON format is incorrect")
					}
					return
				}
				argsType[1] = mqrpc.MAP
				args[1] = pub.GetMsg()
			} else {
				argsType[1] = mqrpc.BYTES
				args[1] = pub.GetMsg()
			}
			session := this.GetSession().Clone()
			session.SetTopic(*pub.GetTopic())
			if msgid != "" {
				argsType[0] = RPCParamSessionType
				b, err := session.Serializable()
				if err != nil {
					return
				}
				args[0] = b
				ctx, _ := context.WithTimeout(context.TODO(), this.module.GetApp().Options().RPCExpired)
				result, e := serverSession.CallArgs(ctx, topics[1], argsType, args)
				toResult(this, *pub.GetTopic(), result, e.Error())
			} else {
				argsType[0] = RPCParamSessionType
				b, err := session.Serializable()
				if err != nil {
					return
				}
				args[0] = b

				e := serverSession.CallNRArgs(topics[1], argsType, args)
				if e != nil {
					log.Warning("Gate rpc", e.Error())
				}
			}
		}
	case mqtt.PINGREQ:
		//客户端发送的心跳包
		//if this.GetSession().GetUserId() != "" {
		//这个链接已经绑定Userid
		//this.lock.Lock()
		//interval := int64(this.lastStorageHeartbeatDataTime) + int64(this.gate.Options().Heartbeat) //单位纳秒
		//this.lock.Unlock()
		//if interval < time.Now().UnixNano() {
		//	if this.gate.GetStorageHandler() != nil {
		//		this.lock.Lock()
		//		this.lastStorageHeartbeatDataTime = time.Duration(time.Now().UnixNano())
		//		this.lock.Unlock()
		//		this.gate.GetStorageHandler().Heartbeat(this.GetSession())
		//	}
		//}
		//}
	}
}

func (this *agent) WriteMsg(topic string, body []byte) error {
	if this.client == nil {
		return errors.New("mqtt.Client nil")
	}
	this.sendNum++
	if hook := this.gate.GetSendMessageHook(); hook != nil {
		bb, err := hook(this.GetSession(), topic, body)
		if err != nil {
			return err
		}
		body = bb
	}
	return this.client.WriteMsg(topic, body)
}

func (this *agent) Close() {
	go func() {
		//关闭连接部分情况下会阻塞超时，因此放协程去处理
		if this.conn != nil {
			this.conn.Close()
		}
	}()
}

func (this *agent) Destroy() {
	if this.conn != nil {
		this.conn.Destroy()
	}
}
