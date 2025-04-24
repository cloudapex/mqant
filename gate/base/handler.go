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

// Package basegate handler
package gatebase

import (
	"context"
	"fmt"
	"runtime"

	"sync"

	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/log"
	"github.com/pkg/errors"
)

// NewGateHandler NewGateHandler
func NewGateHandler(gate gate.Gate) *handler {
	handler := &handler{
		gate: gate,
	}
	return handler
}

// handler GateHandler
type handler struct {
	//gate.AgentLearner
	//gate.GateHandler

	gate     gate.Gate
	lock     sync.RWMutex
	sessions sync.Map //连接列表
	agentNum int      // session size
}

// 当服务关闭时释放
func (h *handler) OnDestroy() {
	h.sessions.Range(func(key, value interface{}) bool {
		value.(gate.Agent).Close()
		h.sessions.Delete(key)
		return true
	})
}

// GetAgentNum
func (h *handler) GetAgentNum() int {
	num := 0
	h.lock.RLock()
	num = h.agentNum
	h.lock.RUnlock()
	return num
}

// GetAgent
func (h *handler) GetAgent(sessionId string) (gate.Agent, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, errors.New("No Sesssion found")
	}
	return agent.(gate.Agent), nil
}

// ========== AgentLearner

// 当连接建立(握手成功)
func (h *handler) Connect(a gate.Agent) {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("handler Connect panic(%v)\n info:%s", err, string(buff))
		}
	}()
	if a.GetSession() != nil {
		h.sessions.Store(a.GetSession().GetSessionID(), a)
		//已经建联成功的才计算
		if a.IsShaked() { // 握手
			h.lock.Lock()
			h.agentNum++
			h.lock.Unlock()
		}
	}
	if h.gate.GetSessionLearner() != nil {
		go func() {
			h.gate.GetSessionLearner().Connect(a.GetSession())
		}()
	}
}

// 当连接关闭(客户端主动关闭或者异常断开)
func (h *handler) DisConnect(a gate.Agent) {
	defer func() {
		if err := recover(); err != nil {
			buff := make([]byte, 1024)
			runtime.Stack(buff, false)
			log.Error("handler DisConnect panic(%v)\n info:%s", err, string(buff))
		}
		if a.GetSession() != nil {
			h.sessions.Delete(a.GetSession().GetSessionID())
			//已经建联成功的才计算
			if a.IsShaked() { // 握手
				h.lock.Lock()
				h.agentNum--
				h.lock.Unlock()
			}
		}
	}()
	if h.gate.GetSessionLearner() != nil {
		if a.GetSession() != nil {
			//没有session的就不返回了
			h.gate.GetSessionLearner().DisConnect(a.GetSession())
		}
	}
}

// ========== Session RPC方法回调

// 获取最新Session数据
func (h *handler) OnRpcUpdLoad(ctx context.Context, sessionId string) (gate.Session, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	return agent.(gate.Agent).GetSession(), nil
}

// Bind the session with the the userId.
func (h *handler) OnRpcBind(ctx context.Context, sessionId string, userId string) (gate.Session, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	agent.(gate.Agent).GetSession().SetUserID(userId)

	if h.gate.GetStorageHandler() != nil && agent.(gate.Agent).GetSession().GetUserID() != "" {
		//可以持久化
		data, err := h.gate.GetStorageHandler().Query(userId)
		if err == nil && data != nil {
			//有已持久化的数据,可能是上一次连接保存的
			h.gate.GetApp()
			impSession, err := NewSession(h.gate.GetApp(), data)
			if err == nil {
				if agent.(gate.Agent).GetSession() == nil {
					agent.(gate.Agent).GetSession().SetSettings(impSession.CloneSettings())
				} else {
					//合并两个map 并且以 agent.(Agent).GetSession().Settings 已有的优先
					settings := impSession.CloneSettings()
					_ = agent.(gate.Agent).GetSession().ImportSettings(settings)
				}
			} else {
				//解析持久化数据失败
				log.Warning("Sesssion Resolve fail %s", err.Error())
			}
		}
		//数据持久化
		_ = h.gate.GetStorageHandler().Storage(agent.(gate.Agent).GetSession())
	}

	return agent.(gate.Agent).GetSession(), nil
}

// UnBind the session with the the userId.
func (h *handler) OnRpcUnBind(ctx context.Context, sessionId string) (gate.Session, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	agent.(gate.Agent).GetSession().SetUserID("")
	return agent.(gate.Agent).GetSession(), nil
}

// Push the session with the the userId.
func (h *handler) OnRpcPush(ctx context.Context, sessionId string, settings map[string]string) (gate.Session, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	// 覆盖当前map对应的key-value
	for key, value := range settings {
		_ = agent.(gate.Agent).GetSession().Set(key, value)
	}
	result := agent.(gate.Agent).GetSession()
	if h.gate.GetStorageHandler() != nil && agent.(gate.Agent).GetSession().GetUserID() != "" {
		err := h.gate.GetStorageHandler().Storage(agent.(gate.Agent).GetSession())
		if err != nil {
			log.Warning("gate session storage failure : %s", err.Error())
		}
	}

	return result, nil
}

// Set values (one or many) for the session.
func (h *handler) OnRpcSet(ctx context.Context, sessionId string, key string, value string) (gate.Session, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}

	_ = agent.(gate.Agent).GetSession().Set(key, value)
	result := agent.(gate.Agent).GetSession()

	if h.gate.GetStorageHandler() != nil && agent.(gate.Agent).GetSession().GetUserID() != "" {
		err := h.gate.GetStorageHandler().Storage(agent.(gate.Agent).GetSession())
		if err != nil {
			log.Error("gate session storage failure : %s", err.Error())
		}
	}

	return result, nil
}

// Del value from the session.
func (h *handler) OnRpcDel(ctx context.Context, sessionId string, key string) (gate.Session, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	_ = agent.(gate.Agent).GetSession().Del(key)
	result := agent.(gate.Agent).GetSession()

	if h.gate.GetStorageHandler() != nil && agent.(gate.Agent).GetSession().GetUserID() != "" {
		err := h.gate.GetStorageHandler().Storage(agent.(gate.Agent).GetSession())
		if err != nil {
			log.Error("gate session storage failure :%s", err.Error())
		}
	}

	return result, nil
}

// Send message to the session.
func (h *handler) OnRpcSend(ctx context.Context, sessionId string, topic string, body []byte) (bool, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return false, fmt.Errorf("No Sesssion found")
	}
	// 组装一个pack{topic,data}
	err := agent.(gate.Agent).SendPack(&gate.Pack{Topic: topic, Body: body})
	if err != nil {
		return false, err
	}
	return true, nil
}

// broadcast message to all session of the gate
func (h *handler) OnRpcBroadCast(ctx context.Context, topic string, body []byte) (int64, error) {
	var count int64 = 0
	h.sessions.Range(func(key, agent interface{}) bool {
		e := agent.(gate.Agent).SendPack(&gate.Pack{Topic: topic, Body: body})
		if e != nil {
			log.Warning("WriteMsg error:", e.Error())
		} else {
			count++
		}
		return true
	})
	return count, nil
}

// 检查连接是否正常
func (h *handler) OnRpcConnected(ctx context.Context, sessionId string) (bool, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return false, fmt.Errorf("No Sesssion found")
	}
	return agent.(gate.Agent).IsClosed(), nil
}

// 主动关闭连接
func (h *handler) OnRpcClose(ctx context.Context, sessionId string) (bool, error) {
	agent, ok := h.sessions.Load(sessionId)
	if !ok || agent == nil {
		return false, fmt.Errorf("No Sesssion found")
	}
	agent.(gate.Agent).Close()
	return true, nil
}
