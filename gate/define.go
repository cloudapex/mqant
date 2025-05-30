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

// Package gate 长连接网关定义
package gate

import (
	"context"
	"time"

	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqrpc"
	"github.com/liangdas/mqant/network"
)

// RPCParamSessionType gate.session 类型
var RPCParamSessionType = "SESSION"

// RPCParamProtocolMarshalType ProtocolMarshal类型
var RPCParamProtocolMarshalType = "ProtocolMarshal"

const (
	// RPC_CLIENT_MSG RPC处理来自客户端的消息
	RPC_CLIENT_MSG string = "RPC_CLIENT_MSG"

	PACK_HEAD_TOTAL_LEN_SIZE  = 2 // 包头中这几个字节存放总pack的长度值
	PACK_HEAD_MSG_ID_LEN_SIZE = 2 // 包头中这几个字节存放msgId的长度值
)

// 定义需要RPC传输session的ContextKey
var ContextTransSession = mqrpc.ContextTransKey("ContextTransSession")

// ContextTransSession快捷WithValue方法
func ContextWithSession(ctx context.Context, session ISession) context.Context {
	return context.WithValue(ctx, ContextTransSession, session)
}

// Pack 消息包
type Pack struct {
	Topic string
	Body  []byte
}

// IGate 网关代理定义
type IGate interface {
	module.IRPCModule

	Options() Options

	GetGateHandler() GateHandler
	GetAgentLearner() AgentLearner
	GetSessionLearner() SessionLearner
	GetStorageHandler() StorageHandler
	GetRouteHandler() RouteHandler
	GetSendMessageHook() SendMessageHook
	GetGuestJudger() func(session ISession) bool
}

// GateHandler 代理服务处理器
type GateHandler interface {
	GetAgent(sessionId string) (IAgent, error)
	GetAgentNum() int
	OnDestroy() // 退出事件,当主动关闭时释放所有的连接

	// 获取最新Session数据
	OnRpcLoad(ctx context.Context, sessionId string) (ISession, error)

	// Bind the session with the the userId.
	OnRpcBind(ctx context.Context, sessionId string, userId string) (ISession, error)

	// UnBind the session with the the userId.
	OnRpcUnBind(ctx context.Context, sessionId string) (ISession, error)

	// Upd settings map value for the session.
	OnRpcPush(ctx context.Context, sessionId string, settings map[string]string) (ISession, error)

	// Set values (one or many) for the session.
	OnRpcSet(ctx context.Context, sessionId string, key string, value string) (ISession, error)

	// Del value from the session.
	OnRpcDel(ctx context.Context, sessionId string, key string) (ISession, error)

	// Send message to the session.
	OnRpcSend(ctx context.Context, sessionId string, topic string, body []byte) (bool, error)

	// 广播消息给网关所有在连客户端
	OnRpcBroadcast(ctx context.Context, topic string, body []byte) (int64, error)

	// 检查连接是否正常
	OnRpcConnected(ctx context.Context, sessionId string) (bool, error)

	// 主动关闭连接
	OnRpcClose(ctx context.Context, sessionId string) (bool, error)
}

// ISession session代表一个客户端连接,不是线程安全的
type ISession interface {
	mqrpc.Marshaler

	// for module.CtxSessionSetApp
	GetApp() module.IApp
	SetApp(module.IApp)

	// --------------- 固定属性区(Gate管理,理论上不可更改)

	GetIP() string
	SetIP(ip string)

	GetTopic() string
	SetTopic(topic string)

	GetNetwork() string
	SetNetwork(network string)

	GetUserID() string
	GetUserIDInt64() int64
	SetUserID(userId string)

	GetSessionID() string
	SetSessionID(sessionId string)

	// GateServerId
	GetServerID() string
	SetServerID(serverId string)

	// 网关本地的额外数据,不会再rpc中传递
	GetLocalUserData() interface{}
	// 网关本地的额外数据,不会再rpc中传递
	SetLocalUserData(data interface{})

	// --------------- Setting区(线程安全)

	Get(key string) (string, bool)
	Set(key, value string) error
	Del(key string) error
	SetSettings(settings map[string]string)
	// 合并两个map 并且以 agent.(Agent).GetSession().Settings 已有的优先
	ImportSettings(map[string]string) error
	//SettingsRange 配合一个回调函数进行遍历操作，通过回调函数返回内部遍历出来的值。回调函数的返回值：需要继续迭代遍历时，返回 true；终止迭代遍历时，返回 false。
	SettingsRange(func(k, v string) bool)

	// 每次rpc调用都拷贝一份新的Session进行传输
	Clone() ISession
	// 只Clone Settings
	CloneSettings() map[string]string

	//是否是访客(未登录)
	IsGuest() bool

	// 生成RPC方法需要的context
	GenRPCContext() context.Context

	// 日志追踪
	UpdTraceSpan()
	GetTraceSpan() log.TraceSpan

	// --------------- Session RPC方法封装

	// update local Session(从Gate拉取最新数据)
	ToUpdate() error
	// Bind the session with the the userId.
	ToBind(userId string) error
	// UnBind the session with the the userId.
	ToUnBind() error
	// Set values (one) for the session.
	ToSet(key string, value string) error
	// Set values (many) for the session(合并已存在的,直接用参数Push)
	ToSetBatch(settings map[string]string) error
	// Push all Settings values for the session(合并已存在的,拿自己的Settings去Push).
	ToPush() error
	// Remove value from the session.
	ToDel(key string) error
	// Send message to the session.
	ToSend(topic string, body []byte) error
	// Send batch message to the sessions(sessionId之间用,分割).
	//ToSendBatch(sessionids string, topic string, body []byte) (int64, error)
	// the session is connect status
	ToConnected() (bool, error)
	// close the session connect
	ToClose() error
}

// IAgent 客户端代理定义
type IAgent interface {
	Init(impl IAgent, gate IGate, conn network.Conn) error
	Close()
	OnClose() error
	Destroy() // 不建议使用,优先使用Close

	Run() (err error)

	ConnTime() time.Time  // 建立连接的时间
	IsClosed() bool       // 连接状态
	IsShaked() bool       // 连接就绪(有些协议会在连接成功后要先握手)
	RecvNum() int64       // 接收消息的数量
	SendNum() int64       // 发送消息的数量
	GetSession() ISession // 管理的ClientSession

	// 发送数据
	SendPack(pack *Pack) error

	// 发送编码Pack后的数据
	OnWriteEncodingPack(pack *Pack) []byte

	// 读取数据并解码出Pack
	OnReadDecodingPack() (*Pack, error)

	// 自行实现如何处理收到的数据包
	OnHandRecvPack(pack *Pack) error

	GetError() error //连接断开的错误日志
}

// StorageHandler Session信息持久化
type StorageHandler interface {
	/**
	存储用户的Session信息
	Session Bind Userid以后每次设置 settings都会调用一次Storage
	*/
	Storage(session ISession) (err error)
	/**
	强制删除Session信息
	*/
	Delete(session ISession) (err error)
	/**
	获取用户Session信息
	Bind Userid时会调用Query获取最新信息
	*/
	Query(Userid string) (data []byte, err error)
	/**
	用户心跳,一般用户在线时1s发送一次
	可以用来延长Session信息过期时间
	*/
	Heartbeat(session ISession)
}

// RouteHandler 路由器
type RouteHandler interface {
	/**
	是否需要对本次客户端请求转发规则进行hook
	*/
	OnRoute(session ISession, topic string, msg []byte) (bool, error)
}

// SendMessageHook 给客户端下发消息拦截器
type SendMessageHook func(session ISession, topic string, msg []byte) ([]byte, error)

// GenResponseHandler 回应处理器
type GenResponseHandler interface {
}

// AgentLearner 连接代理
type AgentLearner interface {
	Connect(a IAgent)    //当连接建立  并且MQTT协议握手成功
	DisConnect(a IAgent) //当连接关闭	或者客户端主动发送MQTT DisConnect命令
}

// SessionLearner 客户端代理
type SessionLearner interface {
	Connect(a ISession)    //当连接建立  并且MQTT协议握手成功
	DisConnect(a ISession) //当连接关闭	或者客户端主动发送MQTT DisConnect命令
}
