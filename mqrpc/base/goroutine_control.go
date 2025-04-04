package rpcbase

import "github.com/liangdas/mqant/mqrpc"

func NewGoroutineControl(size uint32) mqrpc.GoroutineControl {
	control := GtControl{
		listCtr: make(chan int, size),
		maxSize: size,
	}
	return &control
}

type GtControl struct {
	listCtr  chan int
	maxSize  uint32
	lastTime int64
}

func (g *GtControl) Wait() error {
	select {
	case g.listCtr <- 1:
	}
	return nil
}

func (g *GtControl) Finish() {
	select {
	case <-g.listCtr:
	default:
		return
	}
}

func (g *GtControl) GetMax() uint32 {
	return g.maxSize
}
