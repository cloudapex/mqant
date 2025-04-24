package gatebase

import "github.com/liangdas/mqant/gate"

func NewWSAgent() gate.Agent {
	return &WSAgent{}
}

type WSAgent struct {
	agent
}
