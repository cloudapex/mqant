// Package server is an interface for a micro server
package server

import (
	"context"

	"github.com/liangdas/mqant/conf"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqrpc"
	"github.com/pborman/uuid"
)

// Server Server
type Server interface {
	ID() string // 服务节点ID
	Options() Options
	UpdMetadata(key, val string) // 更新元数据(正常需要等到下次注册时生效,如果要立即生效还需要调用ServiceRegister)
	OnInit(module module.Module, app module.IApp, settings *conf.ModuleSettings) error
	OnDestroy() error

	Register(id string, f interface{})   // 注册RPC方法
	RegisterGO(id string, f interface{}) // 注册RPC方法
	SetListener(listener mqrpc.RPCListener)
	ServiceRegister() error   // 向Registry注册自己
	ServiceDeregister() error // 向Registry注销自己

	Start() error
	Stop() error

	String() string
}

// Message RPC消息头
type Message interface {
	Topic() string
	Payload() interface{}
	ContentType() string
}

// Request Request
type Request interface {
	Service() string
	Method() string
	ContentType() string
	Request() interface{}
	// indicates whether the request will be streamed
	Stream() bool
}

// Stream represents a stream established with a client.
// A stream can be bidirectional which is indicated by the request.
// The last error will be left in Error().
// EOF indicated end of the stream.
type Stream interface {
	Context() context.Context
	Request() Request
	Send(interface{}) error
	Recv(interface{}) error
	Error() error
	Close() error
}

// Option Option
type Option func(*Options)

var (
	// DefaultAddress DefaultAddress
	DefaultAddress = ":0"
	// DefaultName DefaultName
	DefaultName = "go-server"
	// DefaultVersion DefaultVersion
	DefaultVersion = "1.0.0"
	// DefaultID DefaultID
	DefaultID = uuid.NewUUID().String()
)

// NewServer returns a new server with options passed in
func NewServer(opt ...Option) Server {
	return newServer(opt...)
}
