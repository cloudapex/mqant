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

// 定义需要RPC传输session的ContextKey
var ContextTransSession = mqrpc.ContextTransKey("ContextTransSession")

// ContextTransSession快捷WithValue方法
func ContextWithSession(ctx context.Context, session gate.Session) context.Context {
	return context.WithValue(ctx, ContextTransSession, session)
}
func init() {
	mqrpc.RegistContextTransValue(ContextTransSession, func() mqrpc.Marshaler { return &sessionAgent{} })
}

type sessionAgent struct {
	app         module.App
	session     *SessionImp
	lock        *sync.RWMutex
	userdata    interface{}
	guestJudger func(session gate.Session) bool
}

// NewSession NewSession
func NewSession(app module.App, data []byte) (gate.Session, error) {
	agent := &sessionAgent{
		app:  app,
		lock: new(sync.RWMutex),
	}
	se := &SessionImp{}
	err := proto.Unmarshal(data, se)
	if err != nil {
		return nil, err
	} // 测试结果
	agent.session = se
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
	err := agent.updateMap(data)
	if err != nil {
		return nil, err
	}
	if agent.session.GetSettings() == nil {
		agent.session.Settings = make(map[string]string)
	}
	return agent, nil
}
func (s *sessionAgent) GetApp() module.App {
	return s.app
}
func (s *sessionAgent) GetIP() string {
	return s.session.GetIP()
}

func (s *sessionAgent) GetTopic() string {
	return s.session.GetTopic()
}

func (s *sessionAgent) GetNetwork() string {
	return s.session.GetNetwork()
}

func (s *sessionAgent) GetUserID() string {
	return s.session.GetUserId()
}

func (s *sessionAgent) GetUserIDInt64() int64 {
	uid64, err := strconv.ParseInt(s.session.GetUserId(), 10, 64)
	if err != nil {
		return -1
	}
	return uid64
}

func (s *sessionAgent) GetSessionID() string {
	return s.session.GetSessionId()
}

func (s *sessionAgent) GetServerID() string {
	return s.session.GetServerId()
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

// ImportSettings 合并两个map 并且以 agent.(Agent).GetSession().Settings 已有的优先
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

func (s *sessionAgent) LocalUserData() interface{} {
	return s.userdata
}

func (s *sessionAgent) SetApp(app module.App) {
	s.app = app
}
func (s *sessionAgent) SetIP(ip string) {
	s.session.IP = ip
}
func (s *sessionAgent) SetTopic(topic string) {
	s.session.Topic = topic
}
func (s *sessionAgent) SetNetwork(network string) {
	s.session.Network = network
}
func (s *sessionAgent) SetUserID(userid string) {
	s.lock.Lock()
	s.session.UserId = userid
	s.lock.Unlock()
}
func (s *sessionAgent) SetSessionID(sessionid string) {
	s.session.SessionId = sessionid
}
func (s *sessionAgent) SetServerID(serverid string) {
	s.session.ServerId = serverid
}
func (s *sessionAgent) SetSettings(settings map[string]string) {
	s.lock.Lock()
	s.session.Settings = settings
	s.lock.Unlock()
}
func (s *sessionAgent) CloneSettings() map[string]string {
	s.lock.Lock()
	defer s.lock.Unlock()
	tmp := map[string]string{}
	for k, v := range s.session.Settings {
		tmp[k] = v
	}
	return tmp
}
func (s *sessionAgent) SetLocalKV(key, value string) error {
	s.lock.Lock()
	s.session.GetSettings()[key] = value
	s.lock.Unlock()
	return nil
}
func (s *sessionAgent) RemoveLocalKV(key string) error {
	s.lock.Lock()
	delete(s.session.GetSettings(), key)
	s.lock.Unlock()
	return nil
}
func (s *sessionAgent) SetLocalUserData(data interface{}) error {
	s.userdata = data
	return nil
}

func (s *sessionAgent) updateMap(datas map[string]interface{}) error {
	Userid := datas["Userid"]
	if Userid != nil {
		s.session.UserId = Userid.(string)
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
	Sessionid := datas["Sessionid"]
	if Sessionid != nil {
		s.session.SessionId = Sessionid.(string)
	}
	Serverid := datas["Serverid"]
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

func (s *sessionAgent) update(session gate.Session) error {
	Userid := session.GetUserID()
	s.session.UserId = Userid

	IP := session.GetIP()
	s.session.IP = IP

	s.session.Topic = session.GetTopic()

	Network := session.GetNetwork()
	s.session.Network = Network

	Sessionid := session.GetSessionID()
	s.session.SessionId = Sessionid

	Serverid := session.GetServerID()
	s.session.ServerId = Serverid

	Settings := map[string]string{}
	session.SettingsRange(func(k, v string) bool {
		Settings[k] = v
		return true
	})
	s.lock.Lock()
	s.session.Settings = Settings
	s.lock.Unlock()
	return nil
}

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

func (s *sessionAgent) Update() (err string) {
	if s.app == nil {
		err = fmt.Sprintf("Module.App is nil")
		return
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		err = fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
		return
	}
	result, _err := server.Call(nil, "Update", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId)
	if _err != nil {
		err = _err.Error()
		if result != nil {
			//绑定成功,重新更新当前Session
			s.update(result.(gate.Session))
		}
	}
	return
}

func (s *sessionAgent) Bind(Userid string) (err string) {
	if s.app == nil {
		err = fmt.Sprintf("Module.App is nil")
		return
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		err = fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
		return
	}
	result, _err := server.Call(nil, "Bind", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId, Userid)
	if _err != nil {
		err = _err.Error()
		if result != nil {
			//绑定成功,重新更新当前Session
			s.update(result.(gate.Session))
		}
	}
	return
}

func (s *sessionAgent) UnBind() (err string) {
	if s.app == nil {
		err = fmt.Sprintf("Module.App is nil")
		return
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		err = fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
		return
	}
	result, _err := server.Call(nil, "UnBind", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId)
	if _err != nil {
		err = _err.Error()
		if result != nil {
			//绑定成功,重新更新当前Session
			s.update(result.(gate.Session))
		}
	}
	return
}

func (s *sessionAgent) Push() (err string) {
	if s.app == nil {
		err = fmt.Sprintf("Module.App is nil")
		return
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		err = fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
		return
	}
	s.lock.Lock()
	tmp := map[string]string{}
	for k, v := range s.session.Settings {
		tmp[k] = v
	}
	s.lock.Unlock()
	result, _err := server.Call(nil, "Push", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId, tmp)
	if _err != nil {
		err = _err.Error()
		if result != nil {
			//绑定成功,重新更新当前Session
			s.update(result.(gate.Session))
		}
	}
	return
}

// 都加一个to
func (s *sessionAgent) Set(key string, value string) (err string) {
	if s.app == nil {
		err = fmt.Sprintf("Module.App is nil")
		return
	}
	result, _err := s.app.Call(nil,
		s.session.ServerId,
		"Set",
		mqrpc.Param(
			log.CreateTrace(s.TraceID(), s.SpanID()),
			s.session.SessionId,
			key,
			value,
		),
	)
	if _err != nil {
		err = _err.Error()
		if result != nil {
			//绑定成功,重新更新当前Session
			s.update(result.(gate.Session))
		}
	}
	return
}
func (s *sessionAgent) SetPush(key string, value string) (err string) {
	if s.app == nil {
		err = fmt.Sprintf("Module.App is nil")
		return
	}
	if s.session.Settings == nil {
		s.session.Settings = map[string]string{}
	}
	s.lock.Lock()
	s.session.Settings[key] = value
	s.lock.Unlock()
	return s.Push()
}
func (s *sessionAgent) SetBatch(settings map[string]string) (err string) {
	if s.app == nil {
		err = fmt.Sprintf("Module.App is nil")
		return
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		err = fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
		return
	}
	result, _err := server.Call(nil, "Push", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId, settings)
	if _err != nil {
		err = _err.Error()
		if result != nil {
			//绑定成功,重新更新当前Session
			s.update(result.(gate.Session))
		}
	}
	return
}
func (s *sessionAgent) Get(key string) (result string) {
	s.lock.RLock()
	if s.session.Settings == nil {
		s.lock.RUnlock()
		return
	}
	result = s.session.Settings[key]
	s.lock.RUnlock()
	return
}

func (s *sessionAgent) Load(key string) (result string, ok bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.session.Settings == nil {
		return "", false
	}
	if result, ok = s.session.Settings[key]; ok {
		return result, ok
	} else {
		return "", false
	}
}

func (s *sessionAgent) Remove(key string) (errStr string) {
	if s.app == nil {
		errStr = fmt.Sprintf("Module.App is nil")
		return
	}
	result, err := s.app.Call(nil,
		s.session.ServerId,
		"Remove",
		mqrpc.Param(
			log.CreateTrace(s.TraceID(), s.SpanID()),
			s.session.SessionId,
			key,
		),
	)
	if err == nil {
		if result != nil {
			//绑定成功,重新更新当前Session
			s.update(result.(gate.Session))
		}
	} else {
		errStr = err.Error()
	}
	return
}
func (s *sessionAgent) Send(topic string, body []byte) string {
	if s.app == nil {
		return fmt.Sprintf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
	}
	_, err := server.Call(nil, "Send", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId, topic, body)
	return err.Error()
}

func (s *sessionAgent) SendBatch(Sessionids string, topic string, body []byte) (int64, string) {
	if s.app == nil {
		return 0, fmt.Sprintf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return 0, fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
	}
	count, err := server.Call(nil, "SendBatch", log.CreateTrace(s.TraceID(), s.SpanID()), Sessionids, topic, body)
	if err != nil {
		return 0, err.Error()
	}
	return count.(int64), ""
}

func (s *sessionAgent) IsConnect(userId string) (bool, string) {
	if s.app == nil {
		return false, fmt.Sprintf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return false, fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
	}
	result, err := server.Call(nil, "IsConnect", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId, userId)
	if err != nil {
		return false, err.Error()
	}
	return result.(bool), ""
}

func (s *sessionAgent) SendNR(topic string, body []byte) string {
	if s.app == nil {
		return fmt.Sprintf("Module.App is nil")
	}
	server, e := s.app.GetServerByID(s.session.ServerId)
	if e != nil {
		return fmt.Sprintf("Service not found id(%s)", s.session.ServerId)
	}
	e = server.CallNR("Send", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId, topic, body)
	if e != nil {
		return e.Error()
	}
	//span:=s.ExtractSpan(topic)
	//if span!=nil{
	//	span.LogEventWithPayload("SendToClient",map[string]string{
	//		"topic":topic,
	//	})
	//	span.Finish()
	//}
	return ""
}

// 每次rpc调用都拷贝一份新的Session进行传输
func (s *sessionAgent) Clone() gate.Session {
	s.lock.Lock()
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
	s.lock.Unlock()
	return agent
}

// ========== TraceLog 部分 考虑移除掉
func (s *sessionAgent) CreateTrace() {
	s.session.TraceId = mqtools.GenerateID().String()
	s.session.SpanId = mqtools.GenerateID().String()
}

func (s *sessionAgent) TraceID() string {
	return s.session.TraceId
}

func (s *sessionAgent) SpanID() string {
	return s.session.SpanId
}

// 是否是访客(未登录), 默认判断规则为(userId=="")
func (s *sessionAgent) IsGuest() bool {
	if s.guestJudger != nil {
		return s.guestJudger(s)
	}
	if s.GetUserID() == "" {
		return true
	}
	return false
}

// 设置自动的访客判断函数,记得一定要在gate模块设置这个值,以免部分模块因为未设置这个判断函数造成错误的判断
func (s *sessionAgent) SetGuestJudger(judger func(session gate.Session) bool) {
	s.guestJudger = judger
}

// ================= RPC方法
func (s *sessionAgent) Close() string {
	if s.app == nil {
		return fmt.Sprintf("Module.App is nil")
	}
	server, err := s.app.GetServerByID(s.session.ServerId)
	if err != nil {
		return fmt.Sprintf("Service not found id(%s), err:%v", s.session.ServerId, err)
	}
	_, err = server.Call(nil, "Close", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId)
	return err.Error()
}

// ToClose 关闭连接
func (s *sessionAgent) ToClose() error {
	if s.app == nil {
		return fmt.Errorf("Module.App is nil")
	}
	server, err := s.app.GetServerByID(s.session.ServerId)
	if err != nil {
		return fmt.Errorf("Service not found id(%s), err:%v", s.session.ServerId, err)
	}
	_, err = server.Call(nil, "Close", log.CreateTrace(s.TraceID(), s.SpanID()), s.session.SessionId)
	return err
}
