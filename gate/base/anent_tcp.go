package gatebase

import "github.com/liangdas/mqant/gate"

func NewTCPAgent() gate.Agent {
	return &TCPAgent{}
}

type TCPAgent struct {
	agent
}
