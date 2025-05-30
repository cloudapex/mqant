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
	"net/http"
	"time"

	"github.com/liangdas/mqant/conf"
	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/module"
	modulebase "github.com/liangdas/mqant/module/base"
	"github.com/liangdas/mqant/network"
)

var _ module.IRPCModule = &ModuleGate{}

type ModuleGate struct {
	modulebase.ModuleBase

	opts gate.Options

	handler     gate.GateHandler                 // 主代理接口
	createAgent func(netTyp string) gate.IAgent  // 创建客户端代理接口
	guestJudger func(session gate.ISession) bool // 是否游客
	shakeHandle func(r *http.Request) error      // 建立连接时鉴权(ws)

	storager        gate.StorageHandler  // Session持久化接口
	router          gate.RouteHandler    // 路由控制接口
	sessionLearner  gate.SessionLearner  // 客户端连接和断开的监听器(业务使用)
	agentLearner    gate.AgentLearner    // 客户端连接和断开的监听器(内部使用)
	sendMessageHook gate.SendMessageHook // 发送消息时的钩子回调
}

func (this *ModuleGate) Init(subclass module.IRPCModule, app module.IApp, settings *conf.ModuleSettings, opts ...gate.Option) {
	this.opts = gate.NewOptions(opts...)
	this.ModuleBase.Init(subclass, app, settings, this.opts.Opts...) // 这是必须的
	if this.opts.WsAddr == "" {
		if WSAddr, ok := settings.Settings["WSAddr"]; ok { // 可以从Settings中配置
			this.opts.WsAddr = WSAddr.(string)
		}
	}
	if this.opts.TCPAddr == "" {
		if TCPAddr, ok := settings.Settings["TCPAddr"]; ok { // 可以从Settings中配置
			this.opts.TCPAddr = TCPAddr.(string)
		}
	}

	if this.opts.TLS == false {
		if tls, ok := settings.Settings["TLS"]; ok { // 可以从Settings中配置
			this.opts.TLS = tls.(bool)
		} else {
			this.opts.TLS = false
		}
	}

	if this.opts.CertFile == "" {
		if CertFile, ok := settings.Settings["CertFile"]; ok { // 可以从Settings中配置
			this.opts.CertFile = CertFile.(string)
		} else {
			this.opts.CertFile = ""
		}
	}

	if this.opts.KeyFile == "" {
		if KeyFile, ok := settings.Settings["KeyFile"]; ok { // 可以从Settings中配置
			this.opts.KeyFile = KeyFile.(string)
		} else {
			this.opts.KeyFile = ""
		}
	}

	handler := NewGateHandler(this)
	this.handler = NewGateHandler(this)
	this.agentLearner = handler
	this.createAgent = this.defaultAgentCreater

	// for session
	this.GetServer().RegisterGO("Load", handler.OnRpcLoad)
	this.GetServer().RegisterGO("Bind", handler.OnRpcBind)
	this.GetServer().RegisterGO("UnBind", handler.OnRpcUnBind)
	this.GetServer().RegisterGO("Push", handler.OnRpcPush)
	this.GetServer().RegisterGO("Set", handler.OnRpcSet)
	this.GetServer().RegisterGO("Del", handler.OnRpcDel)
	this.GetServer().RegisterGO("Send", handler.OnRpcSend)
	this.GetServer().RegisterGO("Connected", handler.OnRpcConnected)
	this.GetServer().RegisterGO("Close", handler.OnRpcClose)
	// send by Broadcast
	this.GetServer().RegisterGO("Broadcast", handler.OnRpcBroadcast)
}
func (this *ModuleGate) OnDestroy() {
	this.ModuleBase.OnDestroy() // 这是必须的
}
func (this *ModuleGate) GetType() string { return "Gate" }

func (this *ModuleGate) Version() string { return "1.0.0" }

func (this *ModuleGate) OnAppConfigurationLoaded(app module.IApp) {
	this.ModuleBase.OnAppConfigurationLoaded(app) // 这是必须的
}
func (this *ModuleGate) OnConfChanged(settings *conf.ModuleSettings) {}

func (this *ModuleGate) Options() gate.Options { return this.opts }

func (this *ModuleGate) Run(closeSig chan bool) {
	var wsServer *network.WSServer
	if this.opts.WsAddr != "" {
		wsServer = new(network.WSServer)
		wsServer.Addr = this.opts.WsAddr
		wsServer.HTTPTimeout = 30 * time.Second
		wsServer.TLS = this.opts.TLS
		wsServer.CertFile = this.opts.CertFile
		wsServer.KeyFile = this.opts.KeyFile
		wsServer.ShakeFunc = this.shakeHandle
		wsServer.MaxMsgLen = uint32(this.opts.MaxPackSize)
		wsServer.NewAgent = func(conn *network.WSConn) network.Agent {
			agent := this.createAgent("ws")
			agent.Init(agent, this, conn)
			return agent
		}
	}

	var tcpServer *network.TCPServer
	if this.opts.TCPAddr != "" {
		tcpServer = new(network.TCPServer)
		tcpServer.Addr = this.opts.TCPAddr
		tcpServer.TLS = this.opts.TLS
		tcpServer.CertFile = this.opts.CertFile
		tcpServer.KeyFile = this.opts.KeyFile
		tcpServer.NewAgent = func(conn *network.TCPConn) network.Agent {
			agent := this.createAgent("tcp")
			agent.Init(agent, this, conn)
			return agent
		}
	}

	if wsServer != nil {
		wsServer.Start()
	}
	if tcpServer != nil {
		tcpServer.Start()
	}
	<-closeSig
	if this.handler != nil {
		this.handler.OnDestroy()
	}
	if wsServer != nil {
		wsServer.Close()
	}
	if tcpServer != nil {
		tcpServer.Close()
	}
}

// 设置创建客户端Agent的函数
func (this *ModuleGate) SetAgentCreater(cfunc func(netTyp string) gate.IAgent) error {
	this.createAgent = cfunc
	return nil
}

// 默认的创建客户端Agent的方法
func (this *ModuleGate) defaultAgentCreater(netTyp string) gate.IAgent {
	switch netTyp {
	case "ws":
		return NewWSAgent()
	case "tcp":
		return NewWSAgent()
	}
	return NewWSAgent()
}

// SetGateHandler 设置代理接口
func (this *ModuleGate) setGateHandler(handler gate.GateHandler) error {
	this.handler = handler
	return nil
}

// GetGateHandler 获取代理接口
func (this *ModuleGate) GetGateHandler() gate.GateHandler { return this.handler }

// SetGuestJudger 设置是否游客的判定器
func (this *ModuleGate) SetGuestJudger(judger func(session gate.ISession) bool) error {
	this.guestJudger = judger
	return nil
}

// GetGuestJudger 获取是否游客的判定器
func (this *ModuleGate) GetGuestJudger() func(session gate.ISession) bool { return this.guestJudger }

// SetShakeHandler 设置建立连接时鉴权器(ws)
func (this *ModuleGate) SetShakeHandler(handler func(r *http.Request) error) error {
	this.shakeHandle = handler
	return nil
}

// GetShakeHandler 获取建立连接时鉴权器(ws)
func (this *ModuleGate) GetShakeHandler() func(r *http.Request) error { return this.shakeHandle }

// SetStorageHandler 设置Session信息持久化接口
func (this *ModuleGate) SetStorageHandler(storager gate.StorageHandler) error {
	this.storager = storager
	return nil
}

// GetStorageHandler 获取Session信息持久化接口
func (this *ModuleGate) GetStorageHandler() (storager gate.StorageHandler) { return this.storager }

// SetRouteHandler 设置路由接口
func (this *ModuleGate) SetRouteHandler(router gate.RouteHandler) error {
	this.router = router
	return nil
}

// GetRouteHandler 获取路由接口
func (this *ModuleGate) GetRouteHandler() gate.RouteHandler { return this.router }

// SetSessionLearner 设置客户端连接和断开的监听器
func (this *ModuleGate) SetSessionLearner(learner gate.SessionLearner) error {
	this.sessionLearner = learner
	return nil
}

// GetSessionLearner 获取客户端连接和断开的监听器
func (this *ModuleGate) GetSessionLearner() gate.SessionLearner { return this.sessionLearner }

// SetAgentLearner 设置客户端连接和断开的监听器
func (this *ModuleGate) setAgentLearner(learner gate.AgentLearner) error {
	this.agentLearner = learner
	return nil
}

// SetAgentLearner 获取客户端连接和断开的监听器(建议用 SetSessionLearner)
func (this *ModuleGate) GetAgentLearner() gate.AgentLearner { return this.agentLearner }

// SetsendMessageHook 设置发送消息时的钩子回调
func (this *ModuleGate) SetSendMessageHook(hook gate.SendMessageHook) error {
	this.sendMessageHook = hook
	return nil
}

// GetSendMessageHook 获取发送消息时的钩子回调
func (this *ModuleGate) GetSendMessageHook() gate.SendMessageHook { return this.sendMessageHook }
