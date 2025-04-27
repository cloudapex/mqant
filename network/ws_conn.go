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

// Package network websocket连接器
package network

import (
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	iptool "github.com/liangdas/mqant/mqtools/ip"
)

// Addr is an implementation of net.Addr for WebSocket.
type Addr struct {
	ip string
}

// Network returns the network type for a WebSocket, "websocket".
func (addr *Addr) Network() string { return "websocket" }
func (addr *Addr) String() string  { return addr.ip }

// WSConn websocket连接
type WSConn struct {
	io.Reader //Read(p []byte) (n int, err error)
	io.Writer //Write(p []byte) (n int, err error)
	sync.Mutex
	header    http.Header // 只保存 请求时的header
	conn      *websocket.Conn
	closeFlag bool
}

func newWSConn(conn *websocket.Conn, r *http.Request) *WSConn {
	wsConn := new(WSConn)
	wsConn.conn = conn
	wsConn.header = r.Header.Clone()
	return wsConn
}

func (wsConn *WSConn) Conn() *websocket.Conn {
	return wsConn.conn
}

func (wsConn *WSConn) doDestroy() {
	wsConn.conn.Close()
	if !wsConn.closeFlag {
		wsConn.closeFlag = true
	}
}

// Destroy 注销连接
func (wsConn *WSConn) Destroy() {
	//wsConn.Lock()
	//defer wsConn.Unlock()

	wsConn.doDestroy()
}

// Close 关闭连接
func (wsConn *WSConn) Close() error {
	//wsConn.Lock()
	//defer wsConn.Unlock()
	if wsConn.closeFlag {
		return nil
	}
	wsConn.closeFlag = true
	return wsConn.conn.Close()
}

// Write Write
func (wsConn *WSConn) Write(p []byte) (int, error) {
	err := wsConn.conn.WriteMessage(websocket.BinaryMessage, p)
	return len(p), err
}

// Read goroutine not safe
func (wsConn *WSConn) Read(p []byte) (n int, err error) {
	_, message, err := wsConn.conn.ReadMessage()
	if err != nil {
		return 0, err
	}

	n = copy(p, message)
	return n, nil
}
func (wsConn *WSConn) ReadMessage() (messageType int, p []byte, err error) {
	return wsConn.conn.ReadMessage()
}

// LocalAddr 获取本地socket地址
func (wsConn *WSConn) LocalAddr() net.Addr {
	return wsConn.conn.LocalAddr()
}

// RemoteAddr 获取远程socket地址
func (wsConn *WSConn) RemoteAddr() net.Addr {
	return &Addr{ip: iptool.RealIP(&http.Request{Header: wsConn.header, RemoteAddr: wsConn.conn.RemoteAddr().String()})}
}

// SetDeadline A zero value for t means I/O operations will not time out.
func (wsConn *WSConn) SetDeadline(t time.Time) error {
	err := wsConn.conn.SetReadDeadline(t)
	if err != nil {
		return err
	}
	return wsConn.conn.SetWriteDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
// A zero value for t means Read will not time out.
func (wsConn *WSConn) SetReadDeadline(t time.Time) error {
	return wsConn.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
// Even if write times out, it may return n > 0, indicating that
// some of the data was successfully written.
// A zero value for t means Write will not time out.
func (wsConn *WSConn) SetWriteDeadline(t time.Time) error {
	return wsConn.conn.SetWriteDeadline(t)
}
