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
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/mqtools"
	"github.com/liangdas/mqant/mqtools/aes"
	"github.com/liangdas/mqant/network"
)

type agentBase struct {
	Impl gate.Agent

	gate                         gate.Gate
	session                      gate.Session
	conn                         network.Conn
	r                            *bufio.Reader
	w                            *bufio.Writer
	ch                           chan int //控制模块可同时开启的最大协程数
	isClosed                     bool
	isShaked                     bool
	lock                         sync.Mutex
	lastStorageHeartbeatDataTime time.Duration //上一次发送存储心跳时间
	recvNum                      int64
	sendNum                      int64
	connTime                     time.Time
	sendPackChan                 chan *gate.Pack // 需要发送的消息缓存
}

func (this *agentBase) Init(impl gate.Agent, gt gate.Gate, conn network.Conn) error {
	this.Impl = impl
	this.ch = make(chan int, gt.Options().ConcurrentTasks)
	this.conn = conn
	this.gate = gt
	this.r = bufio.NewReaderSize(conn, gt.Options().BufSize)
	this.w = bufio.NewWriterSize(conn, gt.Options().BufSize)

	this.isClosed = false
	this.isShaked = false
	this.recvNum = 0
	this.sendNum = 0
	this.sendPackChan = make(chan *gate.Pack, gt.Options().SendPackBuffNum)
	this.lastStorageHeartbeatDataTime = time.Duration(time.Now().UnixNano())
	return nil
}
func (this *agentBase) Close() {
	go func() { // 关闭连接部分情况下会阻塞超时，因此放协程去处理
		if this.conn != nil {
			this.conn.Close()
		}
	}()
}
func (this *agentBase) OnClose() error {
	this.isClosed = true
	close(this.sendPackChan)
	this.gate.GetAgentLearner().DisConnect(this) //发送连接断开的事件
	return nil
}
func (this *agentBase) Destroy() {
	if this.conn != nil {
		this.conn.Destroy()
	}
}
func (this *agentBase) Run() (err error) {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("agent.recvLoop() panic(%v)\n info:%s", err, string(buff))
		}
		this.Close()
	}()

	addr := this.conn.RemoteAddr()
	this.session, err = NewSessionByMap(this.gate.GetApp(), map[string]interface{}{
		"Sessionid": mqtools.GenerateID().String(),
		"Network":   addr.Network(),
		"IP":        addr.String(),
		"Serverid":  this.gate.GetServerID(),
		"Settings":  make(map[string]string),
	})

	this.session.UpdTraceSpan() //代码跟踪
	this.connTime = time.Now()
	this.isShaked = true
	this.gate.GetAgentLearner().Connect(this) //发送连接成功的事件

	log.Info("gate create agent sessionId:%s, current gate agents num:%d", this.session.GetSessionID(), this.gate.GetGateHandler().GetAgentNum())

	go this.sendLoop()     // 发送数据线程
	return this.recvLoop() // 接收数据线程
}

// ========== 属性方法

// ConnTime 建立连接的时间
func (this *agentBase) ConnTime() time.Time { return this.connTime }

// IsClosed 是否关闭了
func (this *agentBase) IsClosed() bool { return this.isClosed }

// IsShaked 连接就绪(握手/认证...)
func (this *agentBase) IsShaked() bool { return this.isShaked }

// RecvNum 接收消息的数量
func (this *agentBase) RecvNum() int64 { return this.recvNum }

// SendNum 发送消息的数量
func (this *agentBase) SendNum() int64 { return this.sendNum }

// GetSession 管理的ClientSession
func (this *agentBase) GetSession() gate.Session { return this.session }

// ========== 处理发送
func (this *agentBase) sendLoop() {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("agent.sendLoop() panic(%v)\n info:%s", err, string(buff))
		}
		this.Close()
	}()

	for pack := range this.sendPackChan {
		this.sendNum++
		sendData := this.Impl.OnWriteEncodingPack(pack)
		this.conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := this.conn.Write(sendData); err != nil {
			log.Error("sendLoop, userId:%v sessionId:%v topic:%v dataLen:%v, err:%v", this.session.GetUserID(), this.session.GetSessionID(), pack.Topic, len(sendData), err)
		} else {
			log.Debug("sendLoop, userId:%v sessionId:%v topic:%v dataLen:%v ok.", this.session.GetUserID(), this.session.GetSessionID(), pack.Topic, len(sendData))
		}
	}
}

// 提供发送数据包的方法
func (this *agentBase) SendPack(pack *gate.Pack) error {
	if this.IsClosed() {
		return nil
	}

	if hook := this.gate.GetSendMessageHook(); hook != nil {
		bb, err := hook(this.GetSession(), pack.Topic, pack.Body)
		if err != nil {
			return err
		}
		pack.Body = bb
	}
	select {
	case this.sendPackChan <- pack:
		return nil
	default:
		return fmt.Errorf("too many unsent messages")
	}
}

// 处理编码Pack后的数据用于发送
func (this *agentBase) OnWriteEncodingPack(pack *gate.Pack) []byte {
	// [普通不加密]
	// headLen := gate.PACK_HEAD_TOTAL_LEN_SIZE + gate.PACK_HEAD_MSG_ID_LEN_SIZE
	// totalLen := headLen + idLen + len(pack.Body)
	// sendData := make([]byte, headLen, totalLen)
	// binary.LittleEndian.PutUint16(sendData, uint16(totalLen))                              // for PACK_HEAD_TOTAL_LEN_SIZE
	// binary.LittleEndian.PutUint16(sendData[gate.PACK_HEAD_TOTAL_LEN_SIZE:], uint16(idLen)) // for PACK_HEAD_MSG_ID_LEN_SIZE
	// sendData = append(sendData, []byte(pack.Topic)...)
	// sendData = append(sendData, pack.Body...)
	// [end]

	idLen := len(pack.Topic)

	// 需要加密的数据: PACK_HEAD_MSG_ID_LEN_SIZE + msgId + msgData
	bodyLen := gate.PACK_HEAD_MSG_ID_LEN_SIZE + idLen + len(pack.Body)
	bodyData := make([]byte, bodyLen)
	binary.LittleEndian.PutUint16(bodyData, uint16(idLen))
	copy(bodyData[gate.PACK_HEAD_MSG_ID_LEN_SIZE:], []byte(pack.Topic))
	copy(bodyData[gate.PACK_HEAD_MSG_ID_LEN_SIZE+idLen:], []byte(pack.Body))

	// 处理加密: 先base加密 + 再ecb加密
	if this.gate.Options().EncryptKey != "" {
		b64Data := base64.StdEncoding.EncodeToString(bodyData)
		encryptedData, err := aes.AES_ECB_Encrypt([]byte(b64Data), []byte(this.gate.Options().EncryptKey))
		if err != nil {
			bodyData = []byte(err.Error())
			log.Error("AES_ECB_Encrypt err:%v", err)
		}
		bodyData = encryptedData
	}

	// 发送数据总长度: PACK_HEAD_TOTAL_LEN_SIZE + len(bodyData)
	totalLen := gate.PACK_HEAD_TOTAL_LEN_SIZE + len(bodyData)
	sendData := make([]byte, totalLen)
	binary.LittleEndian.PutUint16(sendData, uint16(totalLen))
	copy(sendData[gate.PACK_HEAD_TOTAL_LEN_SIZE:], bodyData)
	return sendData
}

// ========== 处理接收
func (this *agentBase) recvLoop() error {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("agent.recvLoop() panic(%v)\n info:%s", err, string(buff))
		}
		this.Close()
	}()

	heartOverTime := this.gate.Options().HeartOverTimer
	for {
		nowTime := time.Now()
		if heartOverTime > 0 {
			_ = this.conn.SetReadDeadline(nowTime.Add(heartOverTime))
		}
		pack, err := this.Impl.OnReadDecodingPack()
		if err != nil {
			if heartOverTime > 0 && time.Since(nowTime) >= (heartOverTime) {
				log.Error("recvLoop heartOverTime, userId:%v sessionId:%v", this.session.GetSessionID(), this.session.GetUserID())
			} else {
				log.Error("recvLoop heartOverTime, userId:%v sessionId:%v err:%s", this.session.GetSessionID(), this.session.GetUserID(), err.Error())
			}
			return err
		}

		if pack == nil {
			continue
		}
		// todo: 把心跳业务放到这里
		if err := this.OnHandRecvPack(pack); err != nil {
			return err
		}
		log.Debug("recvLoop, userId:%v sessionId:%v topic:%v dataLen:%v ok.", this.session.GetUserID(), this.session.GetSessionID(), pack.Topic, len(pack.Body))
	}
}

// 从连接中读取数据并解码出Pack
func (this *agentBase) OnReadDecodingPack() (*gate.Pack, error) {
	return nil, fmt.Errorf("not impl func")
}

// 自行实现如何处理收到的数据包
func (this *agentBase) OnHandRecvPack(pack *gate.Pack) error {
	// 处理保活(默认不处理保活,留给上层处理)

	// 默认是通过topic解析出路由规则
	topic := strings.Split(pack.Topic, "/")
	if len(topic) < 2 {
		return fmt.Errorf("pack.Topic resolving faild with:%v", pack.Topic)
	}
	moduleTyp, msgId := topic[0], topic[1]

	// 优先在已绑定的Module中提供服务
	serverId, _ := this.session.Get(moduleTyp)
	if serverId != "" {
		if server, _ := this.gate.GetApp().GetServerByID(serverId); server != nil {
			_, err := server.Call(this.session.GenRPCContext(), gate.RPC_CLIENT_MSG, msgId, pack.Body)
			return err
		}
	}

	// 然后按照默认路由规则随机取得Module服务
	server, err := this.gate.GetApp().GetRouteServer(moduleTyp)
	if err != nil {
		return fmt.Errorf("Service(moduleType:%s) not found", moduleTyp)
	}

	_, err = server.Call(this.session.GenRPCContext(), gate.RPC_CLIENT_MSG, msgId, pack.Body)
	return err
}

// 获取最后发生的错误
func (this *agentBase) GetError() error {
	return nil
}
