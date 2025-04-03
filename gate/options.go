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

// Package gate 网关配置
package gate

import (
	"time"

	"github.com/liangdas/mqant/server"
)

// Option 网关配置项
type Option func(*Options)

// Options 网关配置项
type Options struct {
	ConcurrentTasks int // 单个连接允许的同时并发协程数,控制流量(20)
	BufSize         int // 连接数据缓存大小(2048)
	MaxPackSize     int // 单个协议包数据最大值(65535)
	TLS             bool
	TCPAddr         string
	WsAddr          string
	CertFile        string
	KeyFile         string
	Heartbeat       time.Duration // 好像不是控制客户端的
	OverTime        time.Duration // 建立连接超时(10s)

	Opts []server.Option // 用来控制Module属性的
}

// NewOptions 网关配置项
func NewOptions(opts ...Option) Options {
	opt := Options{
		Opts:            []server.Option{},
		ConcurrentTasks: 20,
		BufSize:         2048,
		MaxPackSize:     65535,
		Heartbeat:       time.Minute,
		OverTime:        time.Second * 10,
		TLS:             false,
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}

// ConcurrentTasks 设置单个连接允许的同时并发协程数
func ConcurrentTasks(s int) Option {
	return func(o *Options) {
		o.ConcurrentTasks = s
	}
}

// BufSize 单个连接网络数据缓存大小
func BufSize(s int) Option {
	return func(o *Options) {
		o.BufSize = s
	}
}

// MaxPackSize 单个协议包数据最大值
func MaxPackSize(s int) Option {
	return func(o *Options) {
		o.MaxPackSize = s
	}
}

// Heartbeat 心跳时间
func Heartbeat(s time.Duration) Option {
	return func(o *Options) {
		o.Heartbeat = s
	}
}

// OverTime 超时时间
func OverTime(s time.Duration) Option {
	return func(o *Options) {
		o.OverTime = s
	}
}

// Tls Tls
// Deprecated: 因为命名规范问题函数将废弃,请用TLS代替
func Tls(s bool) Option {
	return func(o *Options) {
		o.TLS = s
	}
}

// TLS TLS
func TLS(s bool) Option {
	return func(o *Options) {
		o.TLS = s
	}
}

// TcpAddr tcp监听地址
// Deprecated: 因为命名规范问题函数将废弃,请用TCPAddr代替
func TcpAddr(s string) Option {
	return func(o *Options) {
		o.TCPAddr = s
	}
}

// TCPAddr tcp监听端口
func TCPAddr(s string) Option {
	return func(o *Options) {
		o.TCPAddr = s
	}
}

// WsAddr websocket监听端口
func WsAddr(s string) Option {
	return func(o *Options) {
		o.WsAddr = s
	}
}

// CertFile TLS 证书cert文件
func CertFile(s string) Option {
	return func(o *Options) {
		o.CertFile = s
	}
}

// KeyFile TLS 证书key文件
func KeyFile(s string) Option {
	return func(o *Options) {
		o.KeyFile = s
	}
}

// ServerOpts ServerOpts
func ServerOpts(s []server.Option) Option {
	return func(o *Options) {
		o.Opts = s
	}
}
