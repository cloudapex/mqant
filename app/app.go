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
	"github.com/liangdas/mqant/registry/consul"
	"github.com/liangdas/mqant/selector"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

// NewApp 创建app
func NewApp(opts ...module.Option) module.IApp {
	app := new(DefaultApp)
	app.opts = newOptions(opts...)
	app.opts.Selector.Init(selector.SetWatcher(app.Watcher))
	return app
}

// DefaultApp 默认应用
type DefaultApp struct {
	opts module.Options

	serverList sync.Map

	//将一个RPC调用路由到新的路由上
	serviceRoute func(app module.IApp, route string) string

	configurationLoaded func(app module.IApp)                       // 应用启动配置初始化完成后回调
	moduleInited        func(app module.IApp, module module.Module) // 每个模块初始化完成后回调
	startup             func(app module.IApp)                       // 应用启动完成后回调
}

// 初始化 consule
func (app *DefaultApp) initConsul() error {
	if app.opts.Registry == nil {
		rs := consul.NewRegistry(func(options *registry.Options) {
			options.Addrs = app.opts.ConsulAddr
		})
		app.opts.Registry = rs
		app.opts.Selector.Init(selector.Registry(rs))
	}

	if len(app.opts.ConsulAddr) > 0 {
		log.Info("consul addr :%s", app.opts.ConsulAddr[0])
	}

	return nil
}

// 初始化 config
func (app *DefaultApp) loadConfig() error {
	confData, err := app.Options().Registry.GetKV(app.Options().ConfigKey)
	if err != nil {
		return fmt.Errorf("无法从consul获取配置:%s, err:%v", app.Options().ConfigKey, err)
	}
	err = json.Unmarshal(confData, &conf.Conf)
	if err != nil {
		return fmt.Errorf("consul配置解析失败: err:%v, confData:%s", err.Error(), string(confData))
	}
	return nil
}

// 初始化 nats
func (app *DefaultApp) initNats() error {
	if app.opts.Nats == nil {
		nc, err := nats.Connect(fmt.Sprintf("nats://%s", conf.Conf.Nats.Addr),
			nats.MaxReconnects(conf.Conf.Nats.MaxReconnects))
		if err != nil {
			return fmt.Errorf("initNats err:%v", err)
		}
		app.opts.Nats = nc
	}
	log.Info("nats addr:%s", conf.Conf.Nats.Addr)
	return nil
}

// Run 运行应用
func (app *DefaultApp) Run(mods ...module.Module) error {
	var err error

	// init consul
	err = app.initConsul()
	if err != nil {
		return err
	}

	// init config
	err = app.loadConfig()
	if err != nil {
		return err
	}

	// init log
	log.Init(log.WithDebug(app.opts.Debug),
		log.WithProcessID(app.opts.ProcessEnv),
		log.WithBiDir(app.opts.BIDir),
		log.WithLogDir(app.opts.LogDir),
		log.WithLogFileName(app.opts.LogFileName),
		log.WithBiSetting(conf.Conf.BI),
		log.WithBIFileName(app.opts.BIFileName),
		log.WithLogSetting(conf.Conf.Log))

	if app.configurationLoaded != nil { // callback
		app.configurationLoaded(app)
	}

	// init nats
	err = app.initNats()
	if err != nil {
		return err
	}
	log.Info("mqant %v starting...", app.opts.Version)

	// 1 RegisterRunMod
	manager := modulebase.NewModuleManager()
	manager.RegisterRunMod(modules.TimerModule()) // 先注册时间轮模块 每一个进程都默认运行

	// 2 Register
	for i := 0; i < len(mods); i++ {
		mods[i].OnAppConfigurationLoaded(app)
		manager.Register(mods[i])
	}
	app.OnInit()

	// 2 init modules
	manager.Init(app, app.opts.ProcessEnv)

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

// Config 获取启动配置
func (app *DefaultApp) Config() conf.Config { return conf.Conf }

// Options 获取应用配置
func (app *DefaultApp) Options() module.Options { return app.opts }

// Transporter 获取消息传输对象
func (app *DefaultApp) Transporter() *nats.Conn { return app.opts.Nats }

// Registrar 获取服务注册对象
func (app *DefaultApp) Registrar() registry.Registry { return app.opts.Registry }

// WorkDir 获取进程工作目录
func (app *DefaultApp) WorkDir() string { return app.opts.WorkDir }

// GetProcessEnv 获取应用进程分组环境ID
func (app *DefaultApp) GetProcessEnv() string { return app.opts.ProcessEnv }

// UpdateOptions 允许再次更新应用配置(before app.Run)
func (app *DefaultApp) UpdateOptions(opts ...module.Option) error {
	for _, o := range opts {
		o(&app.opts)
	}
	return nil
}

// Watcher 监视服务节点注销(ServerSession删除掉)
func (app *DefaultApp) Watcher(node *registry.Node) {
	session, ok := app.serverList.Load(node.Id)
	if ok && session != nil {
		session.(module.ServerSession).GetRPC().Done()
		app.serverList.Delete(node.Id)
	}
	// todo: 监视器: go app.serverOb.NotifyDel(svrID)
}

// OnInit 初始化
func (app *DefaultApp) OnInit() error { return nil }

// OnDestroy 应用退出
func (app *DefaultApp) OnDestroy() error { return nil }

// SetServiceRoute 设置服务路由器(动态转换service名称)
func (app *DefaultApp) SetServiceRoute(fn func(app module.IApp, route string) string) error {
	app.serviceRoute = fn
	return nil
}

// GetRouteServer 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
func (app *DefaultApp) GetRouteServer(service string, opts ...selector.SelectOption) (s module.ServerSession, err error) {
	if app.serviceRoute != nil { // 进行一次路由转换
		service = app.serviceRoute(app, service)
	}
	sl := strings.Split(service, "@")
	if len(sl) == 2 {
		serverID := service // sl[0] + @ + sl[1] = moduleType@moduleID
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
		moduleType := serverID // s[0] + @ + s[1] = moduleType@moduleID
		s := strings.Split(serverID, "@")
		if len(s) == 2 {
			moduleType = s[0]
		} else {
			return nil, errors.Errorf("serverID is error %v", serverID)
		}
		sessions := app.GetServersByType(moduleType)
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

// GetServersByType 通过服务类型(moduleType)获取服务实例列表(处理缓存)
func (app *DefaultApp) GetServersByType(moduleType string) []module.ServerSession {
	sessions := make([]module.ServerSession, 0)
	services, err := app.opts.Selector.GetService(moduleType)
	if err != nil {
		log.Warning("GetServersByType %v", err)
		return sessions
	}
	for _, service := range services {
		for _, node := range service.Nodes {
			session, err := app.getServerSessionSafe(node, moduleType)
			if err != nil {
				log.Warning("getServerSessionSafe %v", err)
				continue
			}
			sessions = append(sessions, session.(module.ServerSession))
		}
	}
	return sessions
}

// GetServerBySelector 通过服务类型(moduleType)获取服务实例(可设置选择器)(处理缓存)
func (app *DefaultApp) GetServerBySelector(moduleType string, opts ...selector.SelectOption) (module.ServerSession, error) {
	next, err := app.opts.Selector.Select(moduleType, opts...)
	if err != nil {
		return nil, err
	}
	node, err := next()
	if err != nil {
		return nil, err
	}
	session, err := app.getServerSessionSafe(node, moduleType)
	if err != nil {
		return nil, err
	}
	return session.(module.ServerSession), nil
}

// getServerSessionSafe create and store serverSession safely
func (app *DefaultApp) getServerSessionSafe(node *registry.Node, moduleType string) (module.ServerSession, error) {
	session, ok := app.serverList.Load(node.Id)
	if ok {
		session.(module.ServerSession).SetNode(node)
		return session.(module.ServerSession), nil
	}
	// new
	s, err := modulebase.NewServerSession(app, moduleType, node)
	if err != nil {
		return nil, err
	}
	_session, _ := app.serverList.LoadOrStore(node.Id, s)
	_s := _session.(module.ServerSession)
	if s != _s { // 释放自己创建的那个
		go s.GetRPC().Done()
	}
	return s, nil
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

// Call RPC调用(群发,无需等待结果)
func (app *DefaultApp) CallBroadcast(ctx context.Context, moduleName, _func string, params ...interface{}) {
	listSvr := app.GetServersByType(moduleName)
	for _, svr := range listSvr {
		svr.CallNR(ctx, _func, params...)
	}
}

// --------------- 回调(hook)

// OnConfigurationLoaded 设置应用启动配置初始化完成后回调
func (app *DefaultApp) OnConfigurationLoaded(_func func(app module.IApp)) error {
	app.configurationLoaded = _func
	return nil
}

// GetModuleInited 获取每个模块初始化完成后回调函数
func (app *DefaultApp) GetModuleInited() func(app module.IApp, module module.Module) {
	return app.moduleInited
}

// OnModuleInited 设置每个模块初始化完成后回调
func (app *DefaultApp) OnModuleInited(_func func(app module.IApp, module module.Module)) error {
	app.moduleInited = _func
	return nil
}

// OnStartup 设置应用启动完成后回调
func (app *DefaultApp) OnStartup(_func func(app module.IApp)) error {
	app.startup = _func
	return nil
}
