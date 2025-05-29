// Copyright 2014 loolgame Author. All Rights Reserved.
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

// Package basemodule 服务节点实例定义
package modulebase

import (
	"context"
	"sync"

	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqrpc"
	rpcbase "github.com/liangdas/mqant/mqrpc/base"
	"github.com/liangdas/mqant/registry"
)

// NewServerSession 创建一个节点实例(rpcClient)
func NewServerSession(app module.IApp, name string, node *registry.Node) (module.IServerSession, error) {
	session := &serverSession{
		name: name,
		node: node,
		app:  app,
	}
	rpc, err := rpcbase.NewRPCClient(app, session)
	if err != nil {
		return nil, err
	}
	session.rpc = rpc
	return session, err
}

type serverSession struct {
	mu   sync.RWMutex
	node *registry.Node
	name string
	rpc  mqrpc.RPCClient
	app  module.IApp
}

func (this *serverSession) GetID() string {
	return this.node.Id
}

func (this *serverSession) GetName() string {
	return this.name
}
func (this *serverSession) GetRPC() mqrpc.RPCClient {
	return this.rpc
}

func (this *serverSession) GetApp() module.IApp {
	return this.app
}
func (this *serverSession) GetNode() *registry.Node {
	this.mu.RLock()
	defer this.mu.RUnlock()
	return this.node
}

func (this *serverSession) SetNode(node *registry.Node) (err error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.node = node
	return
}

// 消息请求 需要回复
func (this *serverSession) Call(ctx context.Context, _func string, params ...interface{}) (interface{}, error) {
	return this.rpc.Call(ctx, _func, params...)
}

// 消息请求 不需要回复
func (this *serverSession) CallNR(ctx context.Context, _func string, params ...interface{}) (err error) {
	return this.rpc.CallNR(ctx, _func, params...)
}

// 消息请求 需要回复
func (this *serverSession) CallArgs(ctx context.Context, _func string, argsType []string, args [][]byte) (interface{}, error) {
	return this.rpc.CallArgs(ctx, _func, argsType, args)
}

// 消息请求 不需要回复
func (this *serverSession) CallNRArgs(ctx context.Context, _func string, argsType []string, args [][]byte) (err error) {
	return this.rpc.CallNRArgs(ctx, _func, argsType, args)
}
