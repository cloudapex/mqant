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

// Package basegate gate.Session
package gatebase

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/liangdas/mqant/gate"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqrpc"
	"github.com/liangdas/mqant/mqtools"
	"google.golang.org/protobuf/proto"
)

func init() {
	mqrpc.RegistContextTransValue(gate.ContextTransSession, func() mqrpc.Marshaler {
		s, _ := NewSessionByMap(nil, map[string]interface{}{})
		return s
	})
}

// NewSession NewSession
func NewSession(app module.App, data []byte) (gate.Session, error) {
	agent := &sessionAgent{
		app:  app,
		lock: new(sync.RWMutex),
	}
	err := agent.initByPB(data)
	if err != nil {
		return nil, err
	}
	if agent.session.GetSettings() == nil {
		agent.session.Settings = make(map[string]string)
	}
	return agent, nil
}

// NewSessionByMap NewSessionByMap
func NewSessionByMap(app module.App, data map[string]interface{}) (gate.Session, error) {
	agent := &sessionAgent{
		app:     app,
		session: new(SessionImp),
		lock:    new(sync.RWMutex),
	}
	err := agent.initByMap(data)
	if err != nil {
		return nil, err
	}
	if agent.session.GetSettings() == nil {
		agent.session.Settings = make(map[string]string)
	}
	return agent, nil
}

type sessionAgent struct {
	app         module.App
	session     *SessionImp
	lock        *sync.RWMutex
	userdata    interface{}
	guestJudger func(session gate.Session) bool
}

func (s *sessionAgent) initByPB(data []byte) error {
	se := &SessionImp{}
	err := proto.Unmarshal(data, se)
	if err != nil {
		return err
	}
	s.session = se
	return nil
}
func (s *sessionAgent) initByMap(datas map[string]interface{}) error {
	userId := datas["UserId"]
	if userId != nil {
		s.session.UserId = userId.(string)
	}
	IP := datas["IP"]
	if IP != nil {
		s.session.IP = IP.(string)
	}
	if topic, ok := datas["Topic"]; ok {
		s.session.Topic = topic.(string)
	}
	Network := datas["Network"]
	if Network != nil {
		s.session.Network = Network.(string)
	}
	Sessionid := datas["SessionId"]
	if Sessionid != nil {
		s.session.SessionId = Sessionid.(string)
	}
	Serverid := datas["ServerId"]
	if Serverid != nil {
		s.session.ServerId = Serverid.(string)
	}
	Settings := datas["Settings"]
	if Settings != nil {
		s.lock.Lock()
		s.session.Settings = Settings.(map[string]string)
		s.lock.Unlock()
	}
	return nil
}

func (s *sessionAgent) GetApp() module.App {
	return s.app
}
func (s *sessionAgent) SetApp(app module.App) {
	s.app = app
}
func (s *sessionAgent) GetIP() string {
	return s.session.IP
}
func (s *sessionAgent) SetIP(ip string) {
	s.session.IP = ip
}
func (s *sessionAgent) GetTopic() string {
	return s.session.Topic
}
func (s *sessionAgent) SetTopic(topic string) {
	s.session.Topic = topic
}
func (s *sessionAgent) GetNetwork() string {
	return s.session.Network
}
func (s *sessionAgent) SetNetwork(network string) {
	s.session.Network = network
}
func (s *sessionAgent) GetUserID() string {
	return s.session.UserId
}
func (s *sessionAgent) GetUserIDInt64() int64 {
	uid64, err := strconv.ParseInt(s.GetUserID(), 10, 64)
	if err != nil {
		return -1
	}
	return uid64
}
func (s *sessionAgent) SetUserID(userId string) {
	s.lock.Lock()
	s.session.UserId = userId
	s.lock.Unlock()
}
func (s *sessionAgent) GetSessionID() string {
	return s.session.SessionId
}
func (s *sessionAgent) SetSessionID(sessionId string) {
	s.session.SessionId = sessionId
}
func (s *sessionAgent) GetServerID() string {
	return s.session.ServerId
}
func (s *sessionAgent) SetServerID(serverId string) {
	s.session.ServerId = serverId
}
func (s *sessionAgent) GetLocalUserData() interface{} {
	return s.userdata
}
func (s *sessionAgent) SetLocalUserData(data interface{}) {
	s.userdata = data
}
func (s *sessionAgent) Get(key string) (string, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.session.Settings == nil {
		return "", false
	}
	if result, ok := s.session.Settings[key]; ok {
		return result, ok
	} else {
		return "", false
	}
}
func (s *sessionAgent) Set(key, value string) error {
	s.lock.Lock()
	s.session.Settings[key] = value
	s.lock.Unlock()
	return nil
}
func (s *sessionAgent) Del(key string) error {
	s.lock.Lock()
	delete(s.session.Settings, key)
	s.lock.Unlock()
	return nil
}
func (s *sessionAgent) SetSettings(settings map[string]string) {
	s.lock.Lock()
	s.session.Settings = settings
	s.lock.Unlock()
}

// 合并两个map 并且以 s.Settings 已有的优先
func (s *sessionAgent) ImportSettings(settings map[string]string) error {
	s.lock.Lock()
	if s.session.GetSettings() == nil {
		s.session.Settings = settings
	} else {
		for k, v := range settings {
			if _, ok := s.session.GetSettings()[k]; ok {
				//不用替换
			} else {
				s.session.GetSettings()[k] = v
			}
		}
	}
	s.lock.Unlock()
	return nil
}
func (s *sessionAgent) SettingsRange(f func(k, v string) bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.session.GetSettings() == nil {
		return
	}
	for k, v := range s.session.GetSettings() {
		c := f(k, v)
		if c == false {
			return
		}
	}
}

// update 更新为新的session数据
func (s *sessionAgent) update(session gate.Session) error {
	userId := session.GetUserID()
	s.session.UserId = userId

	ip := session.GetIP()
	s.session.IP = ip

	s.session.Topic = session.GetTopic()

	network := session.GetNetwork()
	s.session.Network = network

	sessionid := session.GetSessionID()
	s.session.SessionId = sessionid

	serverid := session.GetServerID()
	s.session.ServerId = serverid

	settings := map[string]string{}
	session.SettingsRange(func(k, v string) bool {
		settings[k] = v
		return true
	})
	s.lock.Lock()
	s.session.Settings = settings
	s.lock.Unlock()
	return nil
}

// 每次rpc调用都拷贝一份新的Session进行传输
func (s *sessionAgent) Clone() gate.Session {
	s.lock.Lock()
	defer s.lock.Unlock()
	tmp := map[string]string{}
	for k, v := range s.session.Settings {
		tmp[k] = v
	}
	agent := &sessionAgent{
		app:      s.app,
		userdata: s.userdata,
		lock:     new(sync.RWMutex),
		session: &SessionImp{
			IP:        s.session.IP,
			Network:   s.session.Network,
			UserId:    s.session.UserId,
			SessionId: s.session.SessionId,
			ServerId:  s.session.ServerId,
			TraceId:   s.session.TraceId,
			SpanId:    mqtools.GenerateID().String(),
			Settings:  tmp,
		},
	}
	return agent
}

// 只Clone Settings
func (s *sessionAgent) CloneSettings() map[string]string {
	s.lock.Lock()
	defer s.lock.Unlock()
	tmp := map[string]string{}
	for k, v := range s.session.Settings {
		tmp[k] = v
	}
	return tmp
}

// 是否是访客(未登录), 默认判断规则为(userId=="")
func (s *sessionAgent) IsGuest() bool {
	if s.GetUserID() == "" {
		return true
	}
	return false
}

// 生成RPC方法需要的context
func (s *sessionAgent) GenRPCContext() context.Context {
	ctx := context.Background()
	return mqrpc.ContextWithTrace(gate.ContextWithSession(ctx, s), s.GetTraceSpan())
}

// ========== TraceLog 部分
func (s *sessionAgent) UpdTraceSpan() {
	s.session.TraceId = mqtools.GenerateID().String()
	s.session.SpanId = mqtools.GenerateID().String()
}
func (s *sessionAgent) GetTraceSpan() log.TraceSpan {
	return log.CreateTrace(s.session.TraceId, s.session.SpanId)
}

// ========== Session RPC方法封装

// 更新本地Session(其他Module从Gate拉取最新数据)
func (s *sessionAgent) ToUpdate() error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, err := s.app.GetServerByID(s.session.ServerId)
	if err != nil {
		return fmt.Errorf("Gate not found serverId(%s), err:%v", s.session.ServerId, err)
	}
	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "UpdLoad", s.session.SessionId)
	if err != nil {
		return fmt.Errorf("Call Gate serverId(%v) 'Update' err:%v", s.session.ServerId, err)
	}
	if result != nil { // 重新更新当前Session
		s.update(result.(gate.Session))
	}
	return nil
}

// Bind the session with the the userId.
func (s *sessionAgent) ToBind(userId string) error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}
	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Bind", s.session.SessionId, userId)
	if err != nil {
		return fmt.Errorf("Call Gate serverId(%v) 'Bind' err:%v", s.session.ServerId, err)
	}
	if result != nil { // 绑定成功,重新更新当前Session
		s.update(result.(gate.Session))
	}
	return nil
}

// UnBind the session with the the userId.
func (s *sessionAgent) ToUnBind() error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}
	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "UnBind", s.session.SessionId)
	if err != nil {
		return fmt.Errorf("Call Gate serverId(%v) 'UnBind' err:%v", s.session.ServerId, err)
	}
	if result != nil { // 绑定成功,重新更新当前Session
		s.update(result.(gate.Session))
	}
	return nil
}

// Push all Settings values for the session.
func (s *sessionAgent) ToPush() error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}
	s.lock.Lock()
	tmp := map[string]string{}
	for k, v := range s.session.Settings {
		tmp[k] = v
	}
	s.lock.Unlock()
	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Push", s.session.SessionId, tmp)
	if err != nil {
		return fmt.Errorf("Call Gate serverId(%v) 'Push' err:%v", s.session.ServerId, err)
	}
	if result != nil { // 绑定成功,重新更新当前Session
		s.update(result.(gate.Session))
	}
	return nil
}

// Set values (one) for the session.
func (s *sessionAgent) ToSet(key string, value string) error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}

	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Set", s.session.SessionId, s.session.SessionId, key, value)
	if err != nil {
		return fmt.Errorf("Call Gate serverId(%v) 'Set' err:%v", s.session.ServerId, err)
	}
	if result != nil { // 绑定成功,重新更新当前Session
		s.update(result.(gate.Session))
	}
	return nil
}

// Set values (many) for the session.
func (s *sessionAgent) ToSetBatch(settings map[string]string) error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}
	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Push", s.session.SessionId, settings)
	if err != nil {
		return fmt.Errorf("Call Gate serverId(%v) 'Push' err:%v", s.session.ServerId, err)
	}
	if result != nil { // 绑定成功,重新更新当前Session
		s.update(result.(gate.Session))
	}
	return nil
}

// Remove value from the session.
func (s *sessionAgent) ToDel(key string) error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}
	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Del", s.session.SessionId, key)
	if err != nil {
		return fmt.Errorf("Call Gate serverId(%v) 'Remove' err:%v", s.session.ServerId, err)
	}
	if result != nil { // 绑定成功,重新更新当前Session
		s.update(result.(gate.Session))
	}
	return nil
}

// Send message to the session.
func (s *sessionAgent) ToSend(topic string, body []byte) error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}
	return server.CallNR(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Send", s.session.SessionId, topic, body)
}

// the session is connect status
func (s *sessionAgent) ToConnected() (bool, error) {
	if s.app == nil {
		return false, fmt.Errorf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return false, fmt.Errorf("Service not found id(%s)", s.session.ServerId)
	}
	result, err := server.Call(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Connected", s.session.SessionId)
	return result.(bool), err
}

// Close the session connect
func (s *sessionAgent) ToClose() error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, err := s.app.GetServerByID(s.session.ServerId)
	if err != nil {
		return fmt.Errorf("Service not found id(%s), err:%v", s.session.ServerId, err)
	}
	return server.CallNR(mqrpc.ContextWithTrace(context.Background(), s.GetTraceSpan()), "Close", s.session.SessionId)
}

// ========== mqrpc.Marshaler 接口

func (s *sessionAgent) Marshal() ([]byte, error) {
	s.lock.RLock()
	data, err := proto.Marshal(s.session)
	s.lock.RUnlock()
	if err != nil {
		return nil, err
	} // 进行解码
	return data, nil
}
func (s *sessionAgent) Unmarshal(data []byte) error {
	se := &SessionImp{}
	err := proto.Unmarshal(data, se)
	if err != nil {
		return err
	} // 测试结果
	s.session = se
	if s.session.GetSettings() == nil {
		s.lock.Lock()
		s.session.Settings = make(map[string]string)
		s.lock.Unlock()
	}
	return nil
}
func (s *sessionAgent) String() string {
	return "gate.Session"
}
