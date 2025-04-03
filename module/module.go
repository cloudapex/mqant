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

// ProtocolMarshal 数据包装
type ProtocolMarshal interface {
	GetData() []byte
}

// ServerSession 服务代理
type ServerSession interface {
	GetID() string
	GetName() string
	GetRPC() mqrpc.RPCClient
	GetApp() App

	GetNode() *registry.Node
	SetNode(node *registry.Node) (err error)

	Call(ctx context.Context, _func string, params ...interface{}) (interface{}, error)                // 等待返回结果
	CallArgs(ctx context.Context, _func string, ArgsType []string, args [][]byte) (interface{}, error) // 内部使用
	CallNR(_func string, params ...interface{}) (err error)                                            // 无需等待结果
	CallNRArgs(_func string, ArgsType []string, args [][]byte) (err error)                             // 内部使用
}

// App mqant应用定义
type App interface {
	OnInit() error
	OnDestroy() error

	Run(mods ...Module) error
	Configs() conf.Config
	Options() Options
	Transport() *nats.Conn
	Registry() registry.Registry
	WorkDir() string
	GetProcessID() string

	UpdateOptions(opts ...Option) error
	SetMapRoute(fn func(app App, route string) string) error
	GetModuleInited() func(app App, module Module)

	// 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
	GetRouteServer(service string, opts ...selector.SelectOption) (ServerSession, error) //获取经过筛选过的服务
	// 通过服务ID(moduleType@id)获取服务实例
	GetServerByID(serverID string) (ServerSession, error)
	// 通过服务类型(moduleType)获取服务实例列表
	GetServersByType(serviceName string) []ServerSession
	// 通过服务类型(moduleType)获取服务实例(可设置选择器)
	GetServerBySelector(serviceName string, opts ...selector.SelectOption) (ServerSession, error)

	// Call RPC调用(需要等待结果)
	Call(ctx context.Context, moduleType, _func string, param mqrpc.ParamOption, opts ...selector.SelectOption) (interface{}, error)
	// Call RPC调用(无需等待结果)
	CallNR(moduleType string, _func string, params ...interface{}) error

	// 回调
	OnConfigurationLoaded(func(app App)) error
	OnModuleInited(func(app App, module Module)) error
	OnStartup(func(app App)) error

	SetProtocolMarshal(protocolMarshal func(Trace string, Result interface{}, Error string) (ProtocolMarshal, string)) error
	/**
	与客户端通信的协议包接口
	*/
	ProtocolMarshal(Trace string, Result interface{}, Error string) (ProtocolMarshal, string)
	NewProtocolMarshal(data []byte) ProtocolMarshal
}

// Module 基本模块定义
type Module interface {
	GetType() string //模块类型
	Version() string //模块版本
	GetApp() App

	Run(closeSig chan bool)

	OnInit(app App, settings *conf.ModuleSettings)
	OnDestroy()
	OnAppConfigurationLoaded(app App)            // 当App初始化时调用，这个接口不管这个模块是否在这个进程运行都会调用
	OnConfChanged(settings *conf.ModuleSettings) // 为以后动态服务发现做准备(目前没用)
}

// RPCModule RPC模块定义
type RPCModule interface {
	Module
	context.Context

	// 节点ID
	GetServerID() string
	GetModuleSettings() (settings *conf.ModuleSettings)

	// 获取服务实例(通过服务ID|服务类型,可设置选择器过滤)
	GetRouteServer(service string, opts ...selector.SelectOption) (ServerSession, error) //获取经过筛选过的服务
	// 通过服务ID(moduleType@id)获取服务实例
	GetServerByID(serverID string) (ServerSession, error)
	// 通过服务类型(moduleType)获取服务实例列表
	GetServersByType(serviceName string) []ServerSession
	// 通过服务类型(moduleType)获取服务实例(可设置选择器)
	GetServerBySelector(serviceName string, opts ...selector.SelectOption) (ServerSession, error)

	Call(ctx context.Context, moduleType string, _func string, params mqrpc.ParamOption, opts ...selector.SelectOption) (interface{}, error)
	CallNR(moduleType string, _func string, params ...interface{}) error
}
