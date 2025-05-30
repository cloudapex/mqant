// Package httpgateway provides an http-rpc handler which provides the entire http request over rpc
package httpgate

import (
	"context"
	"net/http"

	httpgateapi "github.com/liangdas/mqant/gate/http/api"
	"github.com/liangdas/mqant/gate/http/errors"
	go_api "github.com/liangdas/mqant/gate/http/proto"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqrpc"
)

// APIHandler 网关handler
type APIHandler struct {
	Opts Options
	App  module.IApp
}

// API handler is the default handler which takes api.Request and returns api.Response
func (a *APIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	request, err := httpgateapi.RequestToProto(r)
	if err != nil {
		er := errors.InternalServerError("httpgateway", err.Error())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		w.Write([]byte(er.Error()))
		return
	}
	server, err := a.Opts.Route(a.App, r)
	if err != nil {
		er := errors.InternalServerError("httpgateway", err.Error())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		w.Write([]byte(er.Error()))
		return
	}
	rsp := &go_api.Response{}
	ctx, _ := context.WithTimeout(context.TODO(), a.Opts.TimeOut)
	if err = mqrpc.Proto(rsp, mqrpc.RpcResult(server.SrvSession.Call(ctx, server.Hander, request))); err != nil {
		w.Header().Set("Content-Type", "application/json")
		ce := errors.Parse(err.Error())
		switch ce.Code {
		case 0:
			w.WriteHeader(500)
		default:
			w.WriteHeader(int(ce.Code))
		}
		_, err = w.Write([]byte(ce.Error()))
		return
	} else if rsp.StatusCode == 0 {
		rsp.StatusCode = http.StatusOK
	}

	for _, header := range rsp.GetHeader() {
		for _, val := range header.Values {
			w.Header().Add(header.Key, val)
		}
	}

	if len(w.Header().Get("Content-Type")) == 0 {
		w.Header().Set("Content-Type", "application/json")
	}

	w.WriteHeader(int(rsp.StatusCode))
	w.Write([]byte(rsp.Body))
}

// NewHandler 创建网关
func NewHandler(app module.IApp, opts ...Option) http.Handler {
	options := NewOptions(app, opts...)
	return &APIHandler{
		Opts: options,
		App:  app,
	}
}
