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

	configurationLoaded func(app module.IApp)                              // 应用启动配置初始化完成后回调
	moduleInited        func(app module.IApp, module module.IModule)       // 每个模块初始化完成后回调
	startup             func(app module.IApp)                              // 应用启动完成后回调
	serviceDeleted      func(app module.IApp, moduleName, serverId string) // 当模块服务断开删除时回调
}

// 初始化 consule
func (this *DefaultApp) initConsul() error {
	if this.opts.Registry == nil {
		rs := consul.NewRegistry(func(options *registry.Options) {
			options.Addrs = this.opts.ConsulAddr
		})
		this.opts.Registry = rs
		this.opts.Selector.Init(selector.Registry(rs))
	}

	if len(this.opts.ConsulAddr) > 0 {
		log.Info("consul addr :%s", this.opts.ConsulAddr[0])
	}

	return nil
}

// 初始化 config
func (this *DefaultApp) loadConfig() error {
	confData, err := this.Options().Registry.GetKV(this.Options().ConfigKey)
	if err != nil {
		return fmt.Errorf("无法从consul获取配置:%s, err:%v", this.Options().ConfigKey, err)
	}
	err = json.Unmarshal(confData, &conf.Conf)
	if err != nil {
		return fmt.Errorf("consul配置解析失败: err:%v, confData:%s", err.Error(), string(confData))
	}
	return nil
}

// 初始化 nats
func (this *DefaultApp) initNats() error {
	if this.opts.Nats == nil {
		nc, err := nats.Connect(fmt.Sprintf("nats://%s", conf.Conf.Nats.Addr),
			nats.MaxReconnects(conf.Conf.Nats.MaxReconnects))
		if err != nil {
			return fmt.Errorf("initNats err:%v", err)
		}
		this.opts.Nats = nc
	}
	log.Info("nats addr:%s", conf.Conf.Nats.Addr)
	return nil
}

// Run 运行应用
func (this *DefaultApp) Run(mods ...module.IModule) error {
	var err error

	// init consul
	err = this.initConsul()
	if err != nil {
		return err
	}

	// init config
	err = this.loadConfig()
	if err != nil {
		return err
	}

	// init log
	log.Init(log.WithDebug(this.opts.Debug),
		log.WithProcessID(this.opts.ProcessEnv),
		log.WithBiDir(this.opts.BIDir),
		log.WithLogDir(this.opts.LogDir),
		log.WithLogFileName(this.opts.LogFileName),
		log.WithBiSetting(conf.Conf.BI),
		log.WithBIFileName(this.opts.BIFileName),
		log.WithLogSetting(conf.Conf.Log))

	if this.configurationLoaded != nil { // callback
		this.configurationLoaded(this)
	}

	// init nats
	err = this.initNats()
	if err != nil {
		return err
	}
	log.Info("mqant %v starting...", this.opts.Version)

	// 1 RegisterRunMod
	manager := modulebase.NewModuleManager()
	manager.RegisterRunMod(modules.TimerModule()) // 先注册时间轮模块 每一个进程都默认运行

	// 2 Register
	for i := 0; i < len(mods); i++ {
		mods[i].OnAppConfigurationLoaded(this)
		manager.Register(mods[i])
	}
	this.OnInit()

	// 2 init modules
	manager.Init(this, this.opts.ProcessEnv)

	// 3 startup callback
	if this.startup != nil {
		this.startup(this)
	}
	log.Info("mqant %v started", this.opts.Version)

	// close
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	sig := <-c
	log.BiBeego().Flush()
	log.LogBeego().Flush()

	//如果一分钟都关不了则强制关闭
	timeout := time.NewTimer(this.opts.KillWaitTTL)
	wait := make(chan struct{})
	go func() {
		manager.Destroy()
		this.OnDestroy()
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
func (this *DefaultApp) Config() conf.Config { return conf.Conf }

// Options 获取应用配置
func (this *DefaultApp) Options() module.Options { return this.opts }

// Transporter 获取消息传输对象
func (this *DefaultApp) Transporter() *nats.Conn { return this.opts.Nats }

// Registrar 获取服务注册对象
func (this *DefaultApp) Registrar() registry.Registry { return this.opts.Registry }

// WorkDir 获取进程工作目录
func (this *DefaultApp) WorkDir() string { return this.opts.WorkDir }

// GetProcessEnv 获取应用进程分组环境ID
func (this *DefaultApp) GetProcessEnv() string { return this.opts.ProcessEnv }

// UpdateOptions 允许再次更新应用配置(before this.Run)
func (this *DefaultApp) UpdateOptions(opts ...module.Option) error {
	for _, o := range opts {
		o(&this.opts)
	}
	return nil
}

// Watcher 监视服务节点注销(ServerSession删除掉)
func (this *DefaultApp) Watcher(node *registry.Node) {
	session, ok := this.serverList.Load(node.Id)
	if ok && session != nil {
		session.(module.IServerSession).GetRPC().Done()
		this.serverList.Delete(node.Id)
	}

	// 服务断开回调
	s := strings.Split(node.Id, "@")
	if len(s) < 2 {
		return
	}
	if this.serviceDeleted != nil {
		go this.serviceDeleted(this, s[0], node.Id)
	}
}

// OnInit 初始化
func (this *DefaultApp) OnInit() error { return nil }

// OnDestroy 应用退出
func (this *DefaultApp) OnDestroy() error { return nil }

// SetServiceRoute 设置服务路由器(动态转换service名称)
func (this *DefaultApp) SetServiceRoute(fn func(app module.IApp, route string) string) error {
	this.serviceRoute = fn
	return nil
}

// GetRouteServer 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
func (this *DefaultApp) GetRouteServer(service string, opts ...selector.SelectOption) (module.IServerSession, error) {
	if this.serviceRoute != nil { // 进行一次路由转换
		service = this.serviceRoute(this, service)
	}
	s := strings.Split(service, "@")
	if len(s) == 2 {
		serverID := service // s[0] + @ + s[1] = moduleType@moduleID
		moduleID := s[1]
		if moduleID != "" {
			return this.GetServerByID(serverID)
		}
	}
	moduleType := s[0]
	return this.GetServerBySelector(moduleType, opts...)
}

// GetServerByID 通过服务ID(moduleType@id)获取服务实例
func (this *DefaultApp) GetServerByID(serverID string) (module.IServerSession, error) {
	session, ok := this.serverList.Load(serverID)
	if !ok {
		moduleType := serverID // s[0] + @ + s[1] = moduleType@moduleID
		s := strings.Split(serverID, "@")
		if len(s) == 2 {
			moduleType = s[0]
		} else {
			return nil, errors.Errorf("serverID is error %v", serverID)
		}
		sessions := this.GetServersByType(moduleType)
		for _, s := range sessions {
			if s.GetNode().Id == serverID {
				return s, nil
			}
		}
	} else {
		return session.(module.IServerSession), nil
	}
	return nil, errors.Errorf("nofound %v", serverID)
}

// GetServersByType 通过服务类型(moduleType)获取服务实例列表(处理缓存)
func (this *DefaultApp) GetServersByType(moduleType string) []module.IServerSession {
	sessions := make([]module.IServerSession, 0)
	services, err := this.opts.Selector.GetService(moduleType)
	if err != nil {
		log.Warning("GetServersByType %v", err)
		return sessions
	}
	for _, service := range services {
		for _, node := range service.Nodes {
			session, err := this.getServerSessionSafe(node, moduleType)
			if err != nil {
				log.Warning("getServerSessionSafe %v", err)
				continue
			}
			sessions = append(sessions, session.(module.IServerSession))
		}
	}
	return sessions
}

// GetServerBySelector 通过服务类型(moduleType)获取服务实例(可设置选择器)(处理缓存)
func (this *DefaultApp) GetServerBySelector(moduleType string, opts ...selector.SelectOption) (module.IServerSession, error) {
	next, err := this.opts.Selector.Select(moduleType, opts...)
	if err != nil {
		return nil, err
	}
	node, err := next()
	if err != nil {
		return nil, err
	}
	session, err := this.getServerSessionSafe(node, moduleType)
	if err != nil {
		return nil, err
	}
	return session.(module.IServerSession), nil
}

// getServerSessionSafe create and store serverSession safely
func (this *DefaultApp) getServerSessionSafe(node *registry.Node, moduleType string) (module.IServerSession, error) {
	session, ok := this.serverList.Load(node.Id)
	if ok {
		session.(module.IServerSession).SetNode(node)
		return session.(module.IServerSession), nil
	}
	// new
	s, err := modulebase.NewServerSession(this, moduleType, node)
	if err != nil {
		return nil, err
	}
	_session, _ := this.serverList.LoadOrStore(node.Id, s)
	_s := _session.(module.IServerSession)
	if s != _s { // 释放自己创建的那个
		go s.GetRPC().Done()
	}
	return s, nil
}

// Call RPC调用(需要等待结果)
func (this *DefaultApp) Call(ctx context.Context, moduleType, _func string, param mqrpc.ParamOption, opts ...selector.SelectOption) (result interface{}, err error) {
	server, err := this.GetRouteServer(moduleType, opts...)
	if err != nil {
		return nil, err
	}
	return server.Call(ctx, _func, param()...)
}

// Call RPC调用(无需等待结果)
func (this *DefaultApp) CallNR(ctx context.Context, moduleType, _func string, params ...interface{}) (err error) {
	server, err := this.GetRouteServer(moduleType)
	if err != nil {
		return
	}
	return server.CallNR(ctx, _func, params...)
}

// Call RPC调用(群发,无需等待结果)
func (this *DefaultApp) CallBroadcast(ctx context.Context, moduleName, _func string, params ...interface{}) {
	listSvr := this.GetServersByType(moduleName)
	for _, svr := range listSvr {
		svr.CallNR(ctx, _func, params...)
	}
}

// --------------- 回调(hook)

// OnConfigurationLoaded 设置应用启动配置初始化完成后回调
func (this *DefaultApp) OnConfigurationLoaded(_func func(app module.IApp)) error {
	this.configurationLoaded = _func
	return nil
}

// GetModuleInited 获取每个模块初始化完成后回调函数
func (this *DefaultApp) GetModuleInited() func(app module.IApp, module module.IModule) {
	return this.moduleInited
}

// OnModuleInited 设置每个模块初始化完成后回调
func (this *DefaultApp) OnModuleInited(_func func(app module.IApp, module module.IModule)) error {
	this.moduleInited = _func
	return nil
}

// OnStartup 设置应用启动完成后回调
func (this *DefaultApp) OnStartup(_func func(app module.IApp)) error {
	this.startup = _func
	return nil
}

// OnServiceDeleted 设置当模块服务断开删除时回调
func (this *DefaultApp) OnServiceDeleted(_func func(app module.IApp, moduleName, serverId string)) error {
	this.serviceDeleted = _func
	return nil
}
