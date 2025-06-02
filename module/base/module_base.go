// Copyright 2014 mqantserver Author. All Rights Reserved.
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

// Package basemodule BaseModule定义
package modulebase

import (
	"context"
	"fmt"
	"os"

	"github.com/liangdas/mqant/conf"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/module/server"
	"github.com/liangdas/mqant/module/service"
	"github.com/liangdas/mqant/mqrpc"
	rpcpb "github.com/liangdas/mqant/mqrpc/pb"
	"github.com/liangdas/mqant/mqtools"
	"github.com/liangdas/mqant/selector"
	"github.com/pkg/errors"
)

// ModuleBase 默认的RPCModule实现
type ModuleBase struct {
	context.Context

	App  module.IApp
	Impl module.IRPCModule

	serviceStopeds chan bool
	exit           context.CancelFunc
	settings       *conf.ModuleSettings
	service        service.Service // 内含server
	listener       mqrpc.RPCListener
}

// Init 模块初始化(在OnInit中调用)
func (this *ModuleBase) Init(impl module.IRPCModule, app module.IApp, settings *conf.ModuleSettings, opt ...server.Option) {
	// 初始化模块
	this.App = app
	this.Impl = impl
	this.settings = settings

	// 创建一个远程调用的RPC
	opts := server.Options{
		Metadata: map[string]string{},
	}
	for _, o := range opt {
		o(&opts)
	}
	if opts.Registry == nil {
		opt = append(opt, server.Registry(app.Registrar()))
	}

	if opts.RegisterInterval == 0 {
		opt = append(opt, server.RegisterInterval(app.Options().RegisterInterval))
	}

	if opts.RegisterTTL == 0 {
		opt = append(opt, server.RegisterTTL(app.Options().RegisterTTL))
	}

	if len(opts.Name) == 0 {
		opt = append(opt, server.Name(this.Impl.GetType()))
	}

	if len(opts.ID) == 0 {
		if settings.ID != "" {
			opt = append(opt, server.ID(settings.ID))
		} else {
			opt = append(opt, server.ID(mqtools.GenerateID().String()))
		}
	}

	if len(opts.Version) == 0 {
		opt = append(opt, server.Version(this.Impl.Version()))
	}

	server := server.NewServer(opt...) // opts.Address = nats_server.addr
	err := server.OnInit(this.Impl, app, settings)
	if err != nil {
		log.Warning("server OnInit fail id(%s) error(%s)", this.GetServerID(), err)
	}
	hostname, _ := os.Hostname()
	server.Options().Metadata["hostname"] = hostname
	server.Options().Metadata["pid"] = fmt.Sprintf("%v", os.Getpid())
	ctx, cancel := context.WithCancel(context.Background())
	this.exit = cancel
	this.serviceStopeds = make(chan bool)
	this.service = service.NewService(
		service.Server(server),
		service.RegisterInterval(app.Options().RegisterInterval),
		service.Context(ctx),
	)

	go func() {
		err := this.service.Run()
		if err != nil {
			log.Warning("service run fail id(%s) error(%s)", this.GetServerID(), err)
		}
		close(this.serviceStopeds)
	}()
	this.GetServer().SetListener(this)
}

// OnInit 当模块初始化时调用
func (this *ModuleBase) OnInit(app module.IApp, settings *conf.ModuleSettings) {
	panic("ModuleBase: OnInit() must be implemented")
}

// OnDestroy 当模块注销时调用
func (this *ModuleBase) OnDestroy() {
	//注销模块
	//一定别忘了关闭RPC
	this.exit()
	select {
	case <-this.serviceStopeds:
		// 等待注册中心注销完成
	}
	_ = this.GetServer().OnDestroy()
}

// GetApp 获取app
func (this *ModuleBase) GetApp() module.IApp {
	return this.App
}

// GetImpl 获取子类
func (this *ModuleBase) GetImpl() module.IRPCModule {
	return this.Impl
}

// GetServer server.Server
func (this *ModuleBase) GetServer() server.Server {
	return this.service.Server()
}

// GetServerID 节点ID
func (this *ModuleBase) GetServerID() string {
	//很关键,需要与配置文件中的Module配置对应
	if this.service != nil && this.service.Server() != nil {
		return this.service.Server().ID()
	}
	return "no server"
}

// GetModuleSettings  获取Config.Module[x].Settings
func (this *ModuleBase) GetModuleSettings() *conf.ModuleSettings {
	return this.settings
}

// OnConfChanged 当配置变更时调用(目前没用)
func (this *ModuleBase) OnConfChanged(settings *conf.ModuleSettings) {}

// OnAppConfigurationLoaded 当应用配置加载完成时调用
func (this *ModuleBase) OnAppConfigurationLoaded(app module.IApp) {
	// 当App初始化时调用，这个接口不管这个模块是否在这个进程运行都会调用
	this.App = app
}

// GetRouteServer 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
func (this *ModuleBase) GetRouteServer(service string, opts ...selector.SelectOption) (s module.IServerSession, err error) {
	return this.App.GetRouteServer(service, opts...)
}

// GetServerByID 通过服务ID(moduleType@id)获取服务实例
func (this *ModuleBase) GetServerByID(serverID string) (module.IServerSession, error) {
	return this.App.GetServerByID(serverID)
}

// GetServersByType 通过服务类型(moduleType)获取服务实例列表
func (this *ModuleBase) GetServersByType(serviceName string) []module.IServerSession {
	return this.App.GetServersByType(serviceName)
}

// GetServerBySelector 通过服务类型(moduleType)获取服务实例(可设置选择器)
func (this *ModuleBase) GetServerBySelector(serviceName string, opts ...selector.SelectOption) (module.IServerSession, error) {
	return this.App.GetServerBySelector(serviceName, opts...)
}

// Call  RPC调用(需要等待结果)
func (this *ModuleBase) Call(ctx context.Context, moduleType, _func string, params mqrpc.ParamOption, opts ...selector.SelectOption) (interface{}, error) {
	return this.App.Call(ctx, moduleType, _func, params, opts...)
}

// CallNR  RPC调用(需要等待结果)
func (this *ModuleBase) CallNR(ctx context.Context, moduleType, _func string, params ...interface{}) (err error) {
	return this.App.CallNR(ctx, moduleType, _func, params...)
}

// ================= RPCListener[监听事件]

// SetListener  mqrpc.RPCListener
func (this *ModuleBase) SetListener(listener mqrpc.RPCListener) {
	this.listener = listener
}

// NoFoundFunction  当hander未找到时调用
func (this *ModuleBase) NoFoundFunction(fn string) (*mqrpc.FunctionInfo, error) {
	if this.listener != nil {
		return this.listener.NoFoundFunction(fn)
	}
	return nil, errors.Errorf("Remote function(%s) not found", fn)
}

// BeforeHandle  hander执行前调用
func (this *ModuleBase) BeforeHandle(fn string, callInfo *mqrpc.CallInfo) error {
	if this.listener != nil {
		return this.listener.BeforeHandle(fn, callInfo)
	}
	return nil
}

// OnTimeOut  hander执行超时调用
func (this *ModuleBase) OnTimeOut(fn string, Expired int64) {
	if this.listener != nil {
		this.listener.OnTimeOut(fn, Expired)
	}
}

// OnError  hander执行错误调用
func (this *ModuleBase) OnError(fn string, callInfo *mqrpc.CallInfo, err error) {
	if this.listener != nil {
		this.listener.OnError(fn, callInfo, err)
	}
}

// OnComplete hander成功执行完成时调用
// fn 		方法名
// params		参数
// result		执行结果
// exec_time 	方法执行时间 单位为 Nano 纳秒  1000000纳秒等于1毫秒
func (this *ModuleBase) OnComplete(fn string, callInfo *mqrpc.CallInfo, result *rpcpb.ResultInfo, execTime int64) {
	if this.listener != nil {
		this.listener.OnComplete(fn, callInfo, result, execTime)
	}
}
