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

var _ module.RPCModule = &ModuleGate{}

type ModuleGate struct {
	modulebase.ModuleBase

	opts gate.Options

	handler     gate.GateHandler                // 主代理接口
	createAgent func() gate.Agent               // 创建客户端代理接口
	guestJudger func(session gate.Session) bool // 是否游客
	shakeHandle func(r *http.Request) error     // 建立连接时鉴权(ws)

	storager        gate.StorageHandler  // Session持久化接口
	router          gate.RouteHandler    // 路由控制接口
	sessionLearner  gate.SessionLearner  // 客户端连接和断开的监听器
	agentLearner    gate.AgentLearner    // 客户端连接和断开的监听器(不建议使用)
	sendMessageHook gate.SendMessageHook // 发送消息时的钩子回调
}

func (gt *ModuleGate) Init(subclass module.RPCModule, app module.App, settings *conf.ModuleSettings, opts ...gate.Option) {
	gt.opts = gate.NewOptions(opts...)
	gt.ModuleBase.Init(subclass, app, settings, gt.opts.Opts...) //这是必须的
	if gt.opts.WsAddr == "" {
		if WSAddr, ok := settings.Settings["WSAddr"]; ok { // 可以从Settings中配置
			gt.opts.WsAddr = WSAddr.(string)
		}
	}
	if gt.opts.TCPAddr == "" {
		if TCPAddr, ok := settings.Settings["TCPAddr"]; ok { // 可以从Settings中配置
			gt.opts.TCPAddr = TCPAddr.(string)
		}
	}

	if gt.opts.TLS == false {
		if tls, ok := settings.Settings["TLS"]; ok { // 可以从Settings中配置
			gt.opts.TLS = tls.(bool)
		} else {
			gt.opts.TLS = false
		}
	}

	if gt.opts.CertFile == "" {
		if CertFile, ok := settings.Settings["CertFile"]; ok { // 可以从Settings中配置
			gt.opts.CertFile = CertFile.(string)
		} else {
			gt.opts.CertFile = ""
		}
	}

	if gt.opts.KeyFile == "" {
		if KeyFile, ok := settings.Settings["KeyFile"]; ok { // 可以从Settings中配置
			gt.opts.KeyFile = KeyFile.(string)
		} else {
			gt.opts.KeyFile = ""
		}
	}

	handler := NewGateHandler(gt)
	gt.handler = handler
	gt.agentLearner = handler
	gt.createAgent = gt.defaultAgentCreater

	gt.GetServer().RegisterGO("UpdLoad", gt.handler.OnRpcUpdLoad)
	gt.GetServer().RegisterGO("Bind", gt.handler.OnRpcBind)
	gt.GetServer().RegisterGO("UnBind", gt.handler.OnRpcUnBind)
	gt.GetServer().RegisterGO("Push", gt.handler.OnRpcPush)
	gt.GetServer().RegisterGO("Set", gt.handler.OnRpcSet)
	gt.GetServer().RegisterGO("Del", gt.handler.OnRpcDel)
	gt.GetServer().RegisterGO("Send", gt.handler.OnRpcSend)
	gt.GetServer().RegisterGO("BroadCast", gt.handler.OnRpcBroadCast)
	gt.GetServer().RegisterGO("Connected", gt.handler.OnRpcConnected)
	gt.GetServer().RegisterGO("Close", gt.handler.OnRpcClose)
}
func (gt *ModuleGate) OnDestroy() {
	gt.ModuleBase.OnDestroy() //这是必须的
}
func (gt *ModuleGate) GetType() string { return "Gate" }

func (gt *ModuleGate) Version() string { return "1.0.0" }

func (gt *ModuleGate) OnAppConfigurationLoaded(app module.App) {
	gt.ModuleBase.OnAppConfigurationLoaded(app) //这是必须的
	// err := app.AddRPCSerialize("gate", gt)
	// if err != nil {
	// 	log.Warning("Adding session structures failed to serialize interfaces %s", err.Error())
	// }
}
func (gt *ModuleGate) OnConfChanged(settings *conf.ModuleSettings) {}

func (gt *ModuleGate) Options() gate.Options {
	return gt.opts
}
func (gt *ModuleGate) Run(closeSig chan bool) {
	var wsServer *network.WSServer
	if gt.opts.WsAddr != "" {
		wsServer = new(network.WSServer)
		wsServer.Addr = gt.opts.WsAddr
		wsServer.HTTPTimeout = 30 * time.Second
		wsServer.TLS = gt.opts.TLS
		wsServer.CertFile = gt.opts.CertFile
		wsServer.KeyFile = gt.opts.KeyFile
		wsServer.ShakeFunc = gt.shakeHandle
		wsServer.NewAgent = func(conn *network.WSConn) network.Agent {
			agent := gt.createAgent()
			agent.Init(gt, conn)
			return agent
		}
	}

	var tcpServer *network.TCPServer
	if gt.opts.TCPAddr != "" {
		tcpServer = new(network.TCPServer)
		tcpServer.Addr = gt.opts.TCPAddr
		tcpServer.TLS = gt.opts.TLS
		tcpServer.CertFile = gt.opts.CertFile
		tcpServer.KeyFile = gt.opts.KeyFile
		tcpServer.NewAgent = func(conn *network.TCPConn) network.Agent {
			agent := gt.createAgent()
			agent.Init(gt, conn)
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
	if gt.handler != nil {
		gt.handler.OnDestroy()
	}
	if wsServer != nil {
		wsServer.Close()
	}
	if tcpServer != nil {
		tcpServer.Close()
	}
}

// 设置创建客户端Agent的函数
func (gt *ModuleGate) SetAgentCreater(cfunc func() gate.Agent) error {
	gt.createAgent = cfunc
	return nil
}

// 默认的创建客户端Agent的方法
func (gt *ModuleGate) defaultAgentCreater() gate.Agent {
	a := NewMqttAgent(gt.GetSubclass())
	return a
}

// SetGateHandler 设置代理接口
func (gt *ModuleGate) setGateHandler(handler gate.GateHandler) error {
	gt.handler = handler
	return nil
}

// GetGateHandler 设置代理接口
func (gt *ModuleGate) GetGateHandler() gate.GateHandler {
	return gt.handler
}

// SetGuestJudger 设置是否游客的判定器
func (gt *ModuleGate) SetGuestJudger(judger func(session gate.Session) bool) error {
	gt.guestJudger = judger
	return nil
}

// GetGuestJudger 获取是否游客的判定器
func (gt *ModuleGate) GetGuestJudger() func(session gate.Session) bool {
	return gt.guestJudger
}

// SetShakeHandler 设置建立连接时鉴权器(ws)
func (gt *ModuleGate) SetShakeHandler(handler func(r *http.Request) error) error {
	gt.shakeHandle = handler
	return nil
}

// GetShakeHandler 获取建立连接时鉴权器(ws)
func (gt *ModuleGate) GetShakeHandler() func(r *http.Request) error {
	return gt.shakeHandle
}

// SetStorageHandler 设置Session信息持久化接口
func (gt *ModuleGate) SetStorageHandler(storager gate.StorageHandler) error {
	gt.storager = storager
	return nil
}

// GetStorageHandler 获取Session信息持久化接口
func (gt *ModuleGate) GetStorageHandler() (storager gate.StorageHandler) {
	return gt.storager
}

// SetRouteHandler 设置路由接口
func (gt *ModuleGate) SetRouteHandler(router gate.RouteHandler) error {
	gt.router = router
	return nil
}

// GetRouteHandler 获取路由接口
func (gt *ModuleGate) GetRouteHandler() gate.RouteHandler {
	return gt.router
}

// SetSessionLearner 设置客户端连接和断开的监听器
func (gt *ModuleGate) SetSessionLearner(learner gate.SessionLearner) error {
	gt.sessionLearner = learner
	return nil
}

// GetSessionLearner 获取客户端连接和断开的监听器
func (gt *ModuleGate) GetSessionLearner() gate.SessionLearner {
	return gt.sessionLearner
}

// SetAgentLearner 设置客户端连接和断开的监听器
func (gt *ModuleGate) setAgentLearner(learner gate.AgentLearner) error {
	gt.agentLearner = learner
	return nil
}

// SetAgentLearner 获取客户端连接和断开的监听器(建议用 SetSessionLearner)
func (gt *ModuleGate) GetAgentLearner() gate.AgentLearner {
	return gt.agentLearner
}

// SetsendMessageHook 设置发送消息时的钩子回调
func (gt *ModuleGate) SetSendMessageHook(hook gate.SendMessageHook) error {
	gt.sendMessageHook = hook
	return nil
}

// GetSendMessageHook 获取发送消息时的钩子回调
func (gt *ModuleGate) GetSendMessageHook() gate.SendMessageHook {
	return gt.sendMessageHook
}
