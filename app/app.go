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

// Package app mqant默认应用实现
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/liangdas/mqant/conf"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	modulebase "github.com/liangdas/mqant/module/base"
	"github.com/liangdas/mqant/module/modules"
	"github.com/liangdas/mqant/mqrpc"
	"github.com/liangdas/mqant/registry"
	"github.com/liangdas/mqant/selector"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

// NewApp 创建app
func NewApp(opts ...module.Option) module.App {
	options := newOptions(opts...)
	app := new(DefaultApp)
	app.opts = options
	options.Selector.Init(selector.SetWatcher(app.Watcher))
	return app
}

// DefaultApp 默认应用
type DefaultApp struct {
	version       string
	serverList    sync.Map
	opts          module.Options
	defaultRoutes func(app module.App, Type string, hash string) module.ServerSession
	//将一个RPC调用路由到新的路由上
	mapRoute func(app module.App, route string) string
	//rpcserializes       map[string]mqrpc.RPCSerialize
	configurationLoaded func(app module.App)
	startup             func(app module.App)
	moduleInited        func(app module.App, module module.Module)
	protocolMarshal     func(Trace string, Result interface{}, Error string) (module.ProtocolMarshal, string)
}

// Run 运行应用
func (app *DefaultApp) Run(mods ...module.Module) error {
	conf.LoadConfig(app.opts.ConfPath) //加载配置文件
	cof := conf.Conf                   // app.Configure(cof) //解析配置信息

	if app.configurationLoaded != nil {
		app.configurationLoaded(app)
	}

	// app.AddRPCSerialize("gate", &basegate.SessionSerialize{
	// 	App: app,
	// })
	// log.InitLog(app.opts.Debug, app.opts.ProcessID, app.opts.LogDir, cof.Log)
	// log.InitBI(app.opts.Debug, app.opts.ProcessID, app.opts.BIDir, cof.BI)
	log.Init(log.WithDebug(app.opts.Debug),
		log.WithProcessID(app.opts.ProcessID),
		log.WithBiDir(app.opts.BIDir),
		log.WithLogDir(app.opts.LogDir),
		log.WithLogFileName(app.opts.LogFileName),
		log.WithBiSetting(cof.BI),
		log.WithBIFileName(app.opts.BIFileName),
		log.WithLogSetting(cof.Log))
	log.Info("mqant %v starting...", app.opts.Version)

	// 1 RegisterRunMod
	manager := modulebase.NewModuleManager()
	manager.RegisterRunMod(modules.TimerModule()) // 先注册时间轮模块 每一个进程都默认运行
	// 2 Register
	for i := 0; i < len(mods); i++ {
		mods[i].OnAppConfigurationLoaded(app)
		manager.Register(mods[i])
	}
	// 2 init modules
	app.OnInit()
	manager.Init(app, app.opts.ProcessID)
	// 3 startup callback
	if app.startup != nil {
		app.startup(app)
	}
	log.Info("mqant %v started", app.opts.Version)
	// close
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	sig := <-c
	log.BiBeego().Flush()
	log.LogBeego().Flush()
	//如果一分钟都关不了则强制关闭
	timeout := time.NewTimer(app.opts.KillWaitTTL)
	wait := make(chan struct{})
	go func() {
		manager.Destroy()
		app.OnDestroy()
		wait <- struct{}{}
	}()
	select {
	case <-timeout.C:
		panic(fmt.Sprintf("mqant close timeout (signal: %v)", sig))
	case <-wait:
		log.Info("mqant closing down (signal: %v)", sig)
	}
	log.BiBeego().Close()
	log.LogBeego().Close()
	return nil
}

// Configs 获取应用配置
func (app *DefaultApp) Configs() conf.Config { return conf.Conf }

// Options 获取应用配置
func (app *DefaultApp) Options() module.Options {
	return app.opts
}

// UpdateOptions 更新应用配置(before app.Run)
func (app *DefaultApp) UpdateOptions(opts ...module.Option) error {
	for _, o := range opts {
		o(&app.opts)
	}
	return nil
}

// Transport 获取消息传输对象
func (app *DefaultApp) Transport() *nats.Conn {
	return app.opts.Nats
}

// Registry 获取服务注册对象
func (app *DefaultApp) Registry() registry.Registry {
	return app.opts.Registry
}

// GetProcessID 获取应用分组ID
func (app *DefaultApp) GetProcessID() string {
	return app.opts.ProcessID
}

// WorkDir 获取进程工作目录
func (app *DefaultApp) WorkDir() string {
	return app.opts.WorkDir
}

// Watcher 监视服务节点注销(ServerSession删除掉)
func (app *DefaultApp) Watcher(node *registry.Node) {
	session, ok := app.serverList.Load(node.Id)
	if ok && session != nil {
		session.(module.ServerSession).GetRPC().Done()
		app.serverList.Delete(node.Id)
	}
}

// OnInit 初始化
func (app *DefaultApp) OnInit() error {

	return nil
}

// OnDestroy 应用退出
func (app *DefaultApp) OnDestroy() error {

	return nil
}

// SetMapRoute 设置路由器
func (app *DefaultApp) SetMapRoute(fn func(app module.App, route string) string) error {
	app.mapRoute = fn
	return nil
}

// GetRouteServer 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
func (app *DefaultApp) GetRouteServer(service string, opts ...selector.SelectOption) (s module.ServerSession, err error) {
	if app.mapRoute != nil {
		//进行一次路由转换
		service = app.mapRoute(app, service)
	}
	sl := strings.Split(service, "@")
	if len(sl) == 2 {
		serverID := service
		moduleID := sl[1]
		if moduleID != "" {
			return app.GetServerByID(serverID)
		}
	}
	moduleType := sl[0]
	return app.GetServerBySelector(moduleType, opts...)
}

// GetServerByID 通过服务ID(moduleType@id)获取服务实例
func (app *DefaultApp) GetServerByID(serverID string) (module.ServerSession, error) {
	session, ok := app.serverList.Load(serverID)
	if !ok {
		serviceName := serverID
		s := strings.Split(serverID, "@")
		if len(s) == 2 {
			serviceName = s[0]
		} else {
			return nil, errors.Errorf("serverID is error %v", serverID)
		}
		sessions := app.GetServersByType(serviceName)
		for _, s := range sessions {
			if s.GetNode().Id == serverID {
				return s, nil
			}
		}
	} else {
		return session.(module.ServerSession), nil
	}
	return nil, errors.Errorf("nofound %v", serverID)
}

// GetServersByType 通过服务类型(moduleType)获取服务实例列表
func (app *DefaultApp) GetServersByType(serviceName string) []module.ServerSession {
	sessions := make([]module.ServerSession, 0)
	services, err := app.opts.Selector.GetService(serviceName)
	if err != nil {
		log.Warning("GetServersByType %v", err)
		return sessions
	}
	for _, service := range services {
		//log.TInfo(nil,"GetServersByType3 %v %v",Type,service.Nodes)
		for _, node := range service.Nodes {
			session, ok := app.serverList.Load(node.Id)
			if !ok {
				s, err := modulebase.NewServerSession(app, serviceName, node)
				if err != nil {
					log.Warning("NewServerSession %v", err)
				} else {
					app.serverList.Store(node.Id, s)
					sessions = append(sessions, s)
				}
			} else {
				session.(module.ServerSession).SetNode(node)
				sessions = append(sessions, session.(module.ServerSession))
			}
		}
	}
	return sessions
}

// GetServerBySelector 通过服务类型(moduleType)获取服务实例(可设置选择器)
func (app *DefaultApp) GetServerBySelector(serviceName string, opts ...selector.SelectOption) (module.ServerSession, error) {
	next, err := app.opts.Selector.Select(serviceName, opts...)
	if err != nil {
		return nil, err
	}
	node, err := next()
	if err != nil {
		return nil, err
	}
	session, ok := app.serverList.Load(node.Id)
	if !ok {
		s, err := modulebase.NewServerSession(app, serviceName, node)
		if err != nil {
			return nil, err
		}
		app.serverList.Store(node.Id, s)
		return s, nil
	}
	session.(module.ServerSession).SetNode(node)
	return session.(module.ServerSession), nil

}

// Call RPC调用(需要等待结果)
func (app *DefaultApp) Call(ctx context.Context, moduleType, _func string, param mqrpc.ParamOption, opts ...selector.SelectOption) (result interface{}, err error) {
	server, err := app.GetRouteServer(moduleType, opts...)
	if err != nil {
		return nil, err
	}
	return server.Call(ctx, _func, param()...)
}

// Call RPC调用(无需等待结果)
func (app *DefaultApp) CallNR(ctx context.Context, moduleType, _func string, params ...interface{}) (err error) {
	server, err := app.GetRouteServer(moduleType)
	if err != nil {
		return
	}
	return server.CallNR(ctx, _func, params...)
}

// OnConfigurationLoaded 设置配置初始化完成后回调
func (app *DefaultApp) OnConfigurationLoaded(_func func(app module.App)) error {
	app.configurationLoaded = _func
	return nil
}

// GetModuleInited 获取模块初始化完成后回调函数
func (app *DefaultApp) GetModuleInited() func(app module.App, module module.Module) {
	return app.moduleInited
}

// OnModuleInited 设置模块初始化完成后回调
func (app *DefaultApp) OnModuleInited(_func func(app module.App, module module.Module)) error {
	app.moduleInited = _func
	return nil
}

// OnStartup 设置应用启动完成后回调
func (app *DefaultApp) OnStartup(_func func(app module.App)) error {
	app.startup = _func
	return nil
}

// SetProtocolMarshal 设置RPC数据包装器
func (app *DefaultApp) SetProtocolMarshal(protocolMarshal func(Trace string, Result interface{}, Error string) (module.ProtocolMarshal, string)) error {
	app.protocolMarshal = protocolMarshal
	return nil
}

// ProtocolMarshal RPC数据包装器
func (app *DefaultApp) ProtocolMarshal(Trace string, Result interface{}, Error string) (module.ProtocolMarshal, string) {
	if app.protocolMarshal != nil {
		return app.protocolMarshal(Trace, Result, Error)
	}
	r := &resultInfo{
		Trace:  Trace,
		Error:  Error,
		Result: Result,
	}
	b, err := json.Marshal(r)
	if err == nil {
		return app.NewProtocolMarshal(b), ""
	}
	return nil, err.Error()
}

// NewProtocolMarshal 创建RPC数据包装器
func (app *DefaultApp) NewProtocolMarshal(data []byte) module.ProtocolMarshal {
	return &protocolMarshalImp{
		data: data,
	}
}
