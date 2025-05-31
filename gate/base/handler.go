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

	"sync"

	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/mqtools"
	"github.com/pkg/errors"
)

// NewGateHandler NewGateHandler
func NewGateHandler(gate gate.IGate) *handler {
	return &handler{
		gate: gate,
	}
}

// handler GateHandler
type handler struct {
	//gate.AgentLearner
	//gate.GateHandler

	gate     gate.IGate
	lock     sync.RWMutex
	sessions sync.Map //连接列表
	agentNum int      // session size
}

// 当服务关闭时释放
func (this *handler) OnDestroy() {
	this.sessions.Range(func(key, value interface{}) bool {
		value.(gate.IAgent).Close()
		this.sessions.Delete(key)
		return true
	})
}

// GetAgentNum
func (this *handler) GetAgentNum() int {
	num := 0
	this.lock.RLock()
	num = this.agentNum
	this.lock.RUnlock()
	return num
}

// GetAgent
func (this *handler) GetAgent(sessionId string) (gate.IAgent, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, errors.New("No Sesssion found")
	}
	return agent.(gate.IAgent), nil
}

// ========== AgentLearner

// 当连接建立(握手成功)
func (this *handler) Connect(a gate.IAgent) {
	defer func() {
		if err := mqtools.Catch(recover()); err != nil {
			log.Error("gateHandler Connect(agent) panic:%v", err)
		}
	}()
	if a.GetSession() != nil {
		this.sessions.Store(a.GetSession().GetSessionID(), a)
		// 已经建联成功的才计算
		if a.IsShaked() { // 握手
			this.lock.Lock()
			this.agentNum++
			this.lock.Unlock()
		}
	}
	// 客户端连接和断开的监听器
	if this.gate.GetSessionLearner() != nil {
		go func() {
			this.gate.GetSessionLearner().Connect(a.GetSession())
		}()
	}
}

// 当连接关闭(客户端主动关闭或者异常断开)
func (this *handler) DisConnect(a gate.IAgent) {
	defer func() {
		if err := mqtools.Catch(recover()); err != nil {
			log.Error("handler DisConnect panic:%v", err)
		}
		if a.GetSession() != nil {
			this.sessions.Delete(a.GetSession().GetSessionID())
			// 已经建联成功的才计算
			if a.IsShaked() { // 握手
				this.lock.Lock()
				this.agentNum--
				this.lock.Unlock()
			}
		}
	}()
	// 客户端连接和断开的监听器
	if this.gate.GetSessionLearner() != nil {
		if a.GetSession() != nil {
			// 没有session的就不回调了
			this.gate.GetSessionLearner().DisConnect(a.GetSession())
		}
	}
}

// ========== Session RPC方法回调

// Load the latest session
func (this *handler) OnRpcLoad(ctx context.Context, sessionId string) (gate.ISession, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	return agent.(gate.IAgent).GetSession(), nil
}

// Bind the session with the the userId.
func (this *handler) OnRpcBind(ctx context.Context, sessionId string, userId string) (gate.ISession, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	agent.(gate.IAgent).GetSession().SetUserID(userId)

	if storager := this.gate.GetStorageHandler(); storager != nil && agent.(gate.IAgent).GetSession().GetUserID() != "" {
		// 可以持久化
		data, err := storager.Query(userId)
		if err == nil && data != nil {
			// 有已持久化的数据,可能是上一次连接保存的
			impSession, err := NewSession(this.gate.GetApp(), data)
			if err == nil {
				// 合并两个map 并且以 agent.(Agent).GetSession().Settings 已有的优先
				agent.(gate.IAgent).GetSession().SetSettings(impSession.CloneSettings())
			} else {
				// 解析持久化数据失败
				log.Warning("Sesssion Resolve fail %s", err.Error())
			}
		}
		//数据持久化
		_ = storager.Storage(agent.(gate.IAgent).GetSession())
	}

	return agent.(gate.IAgent).GetSession(), nil
}

// UnBind the session with the the userId.
func (this *handler) OnRpcUnBind(ctx context.Context, sessionId string) (gate.ISession, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	agent.(gate.IAgent).GetSession().SetUserID("")
	return agent.(gate.IAgent).GetSession(), nil
}

// Push the session with the the userId.
func (this *handler) OnRpcPush(ctx context.Context, sessionId string, settings map[string]string) (gate.ISession, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	// 覆盖当前map对应的key-value
	for key, value := range settings {
		_ = agent.(gate.IAgent).GetSession().Set(key, value)
	}
	result := agent.(gate.IAgent).GetSession()
	if storager := this.gate.GetStorageHandler(); storager != nil && agent.(gate.IAgent).GetSession().GetUserID() != "" {
		err := storager.Storage(agent.(gate.IAgent).GetSession())
		if err != nil {
			log.Warning("gate session storage failure : %s", err.Error())
		}
	}

	return result, nil
}

// Set values (one or many) for the session.
func (this *handler) OnRpcSet(ctx context.Context, sessionId string, key string, value string) (gate.ISession, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}

	_ = agent.(gate.IAgent).GetSession().Set(key, value)
	result := agent.(gate.IAgent).GetSession()

	if storager := this.gate.GetStorageHandler(); storager != nil && agent.(gate.IAgent).GetSession().GetUserID() != "" {
		err := storager.Storage(agent.(gate.IAgent).GetSession())
		if err != nil {
			log.Error("gate session storage failure : %s", err.Error())
		}
	}

	return result, nil
}

// Del value from the session.
func (this *handler) OnRpcDel(ctx context.Context, sessionId string, key string) (gate.ISession, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return nil, fmt.Errorf("No Sesssion found")
	}
	_ = agent.(gate.IAgent).GetSession().Del(key)
	result := agent.(gate.IAgent).GetSession()

	if storager := this.gate.GetStorageHandler(); storager != nil && agent.(gate.IAgent).GetSession().GetUserID() != "" {
		err := storager.Storage(agent.(gate.IAgent).GetSession())
		if err != nil {
			log.Error("gate session storage failure :%s", err.Error())
		}
	}

	return result, nil
}

// Send message to the session.
func (this *handler) OnRpcSend(ctx context.Context, sessionId string, topic string, body []byte) (bool, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return false, fmt.Errorf("No Sesssion found")
	}
	// 组装一个pack{topic,data}
	err := agent.(gate.IAgent).SendPack(&gate.Pack{Topic: topic, Body: body})
	if err != nil {
		return false, err
	}
	return true, nil
}

// check connect is normal for the session
func (this *handler) OnRpcConnected(ctx context.Context, sessionId string) (bool, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return false, fmt.Errorf("No Sesssion found")
	}
	return agent.(gate.IAgent).IsClosed(), nil
}

// Proactively close the connection of session
func (this *handler) OnRpcClose(ctx context.Context, sessionId string) (bool, error) {
	agent, ok := this.sessions.Load(sessionId)
	if !ok || agent == nil {
		return false, fmt.Errorf("No Sesssion found")
	}
	agent.(gate.IAgent).Close()
	return true, nil
}

// broadcast message to all session of the gate
func (this *handler) OnRpcBroadcast(ctx context.Context, topic string, body []byte) (int64, error) {
	var count int64 = 0
	this.sessions.Range(func(key, agent interface{}) bool {
		e := agent.(gate.IAgent).SendPack(&gate.Pack{Topic: topic, Body: body})
		if e != nil {
			log.Warning("WriteMsg error:", e.Error())
		} else {
			count++
		}
		return true
	})
	return count, nil
}
