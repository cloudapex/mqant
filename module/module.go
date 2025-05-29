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

// Package module 模块定义
package module

import (
	"context"

	"github.com/liangdas/mqant/conf"
	"github.com/liangdas/mqant/mqrpc"
	"github.com/liangdas/mqant/registry"
	"github.com/liangdas/mqant/selector"
	"github.com/nats-io/nats.go"
)

// IApp mqant应用定义
type IApp interface {
	OnInit() error
	OnDestroy() error

	Run(mods ...IModule) error

	// Config 获取启动配置
	Config() conf.Config

	// Options 获取应用配置
	Options() Options
	// Transporter 获取消息传输对象
	Transporter() *nats.Conn
	// Registrar 获取服务注册对象
	Registrar() registry.Registry
	// WorkDir 获取进程工作目录
	WorkDir() string
	// GetProcessEnv 获取应用进程分组ID(dev,test,...)
	GetProcessEnv() string

	// UpdateOptions 允许再次更新应用配置(before app.Run)
	UpdateOptions(opts ...Option) error
	// 设置服务路由器(动态转换service名称)
	SetServiceRoute(fn func(app IApp, route string) string) error

	// 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
	GetRouteServer(service string, opts ...selector.SelectOption) (IServerSession, error) //获取经过筛选过的服务
	// 通过服务ID(moduleType@id)获取服务实例
	GetServerByID(serverID string) (IServerSession, error)
	// 通过服务类型(moduleType)获取服务实例列表
	GetServersByType(serviceName string) []IServerSession
	// 通过服务类型(moduleType)获取服务实例(可设置选择器)
	GetServerBySelector(serviceName string, opts ...selector.SelectOption) (IServerSession, error)

	// Call RPC调用(需要等待结果)
	Call(ctx context.Context, moduleType, _func string, param mqrpc.ParamOption, opts ...selector.SelectOption) (interface{}, error)
	// Call RPC调用(无需等待结果)
	CallNR(ctx context.Context, moduleType, _func string, params ...interface{}) error
	// Call RPC调用(群发,无需等待结果)
	CallBroadcast(ctx context.Context, moduleName, _func string, params ...interface{})

	// 回调(hook)
	OnConfigurationLoaded(func(app IApp)) error                               // 设置应用启动配置初始化完成后回调
	OnModuleInited(func(app IApp, module IModule)) error                      // 设置每个模块初始化完成后回调
	GetModuleInited() func(app IApp, module IModule)                          // 获取每个模块初始化完成后回调函数
	OnStartup(func(app IApp)) error                                           // 设置应用启动完成后回调
	OnServiceDeleted(_func func(app IApp, moduleName, serverId string)) error // 设置当模块服务断开删除时回调
}

// IModule 基本模块定义
type IModule interface {
	GetApp() IApp

	GetType() string // 模块类型
	Version() string // 模块版本

	Run(closeSig chan bool)

	OnInit(app IApp, settings *conf.ModuleSettings)
	OnDestroy()
	OnAppConfigurationLoaded(app IApp)           // 当App初始化时调用，这个接口不管这个模块是否在这个进程运行都会调用
	OnConfChanged(settings *conf.ModuleSettings) // 为以后动态服务发现做准备(目前没用)
}

// IRPCModule RPC模块定义
type IRPCModule interface {
	IModule
	context.Context

	// 节点ID
	GetServerID() string
	GetModuleSettings() (settings *conf.ModuleSettings)

	// 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
	GetRouteServer(service string, opts ...selector.SelectOption) (IServerSession, error) //获取经过筛选过的服务
	// 通过服务ID(moduleType@id)获取服务实例
	GetServerByID(serverID string) (IServerSession, error)
	// 通过服务类型(moduleType)获取服务实例列表
	GetServersByType(serviceName string) []IServerSession
	// 通过服务类型(moduleType)获取服务实例(可设置选择器)
	GetServerBySelector(serviceName string, opts ...selector.SelectOption) (IServerSession, error)

	Call(ctx context.Context, moduleType, _func string, params mqrpc.ParamOption, opts ...selector.SelectOption) (interface{}, error)
	CallNR(ctx context.Context, moduleType, _func string, params ...interface{}) error
}

// IServerSession 服务代理
type IServerSession interface {
	GetID() string
	GetName() string
	GetRPC() mqrpc.RPCClient
	GetApp() IApp

	GetNode() *registry.Node
	SetNode(node *registry.Node) (err error)

	Call(ctx context.Context, _func string, params ...interface{}) (interface{}, error)                // 等待返回结果
	CallArgs(ctx context.Context, _func string, argsType []string, args [][]byte) (interface{}, error) // 内部使用(ctx参数必须装进args中)
	CallNR(ctx context.Context, _func string, params ...interface{}) (err error)                       // 无需等待结果
	CallNRArgs(ctx context.Context, _func string, argsType []string, args [][]byte) (err error)        // 内部使用(ctx参数必须装进args中)
}

// RPC传输时Context中的数据可能会需要赋值跨服务的app(为什么会有这个接口,会循环import)
type CtxSessionSetApp interface {
	SetApp(IApp)
	GetSessionID() string
}
