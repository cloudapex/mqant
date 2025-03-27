package server

import (
	"strconv"
	"strings"
	"sync"

	"github.com/liangdas/mqant/conf"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqrpc"
	rpcbase "github.com/liangdas/mqant/mqrpc/base"
	"github.com/liangdas/mqant/mqtools/lib/addr"
	"github.com/liangdas/mqant/registry"
)

func newServer(opts ...Option) Server {
	options := newOptions(opts...)
	return &server{
		opts: options,
		exit: make(chan chan error),
	}
}

type server struct {
	exit chan chan error

	sync.RWMutex
	opts Options
	// used for first registration
	registered bool
	server     mqrpc.RPCServer
	id         string // 节点ID
	// graceful exit
	wg sync.WaitGroup
}

func (s *server) ID() string {
	return s.id
}
func (s *server) Options() Options {
	s.RLock()
	opts := s.opts
	s.RUnlock()
	return opts
}
func (s *server) UpdMetadata(key, val string) {
	s.RLock()
	s.opts.Metadata[key] = val
	s.RUnlock()
}
func (s *server) OnInit(module module.Module, app module.App, settings *conf.ModuleSettings) error {
	server, err := rpcbase.NewRPCServer(app, module) //默认会创建一个本地的RPC
	if err != nil {
		log.Warning("Dial: %s", err)
	}
	s.server = server
	s.opts.Address = server.Addr()
	if err := s.ServiceRegister(); err != nil {
		return err
	}
	return nil
}
func (s *server) OnDestroy() error {
	return s.Stop()
}
func (s *server) SetListener(listener mqrpc.RPCListener) { s.server.SetListener(listener) }

func (s *server) Register(id string, f interface{}) {
	if s.server == nil {
		panic("invalid RPCServer")
	}
	s.server.Register(id, f)
}

func (s *server) RegisterGO(id string, f interface{}) {
	if s.server == nil {
		panic("invalid RPCServer")
	}
	s.server.RegisterGO(id, f)
}

// ServiceRegister 向Registry注册自己
func (s *server) ServiceRegister() error {
	// parse address for host, port
	config := s.Options()
	var advt, host string
	var port int

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	parts := strings.Split(advt, ":")
	if len(parts) > 1 {
		host = strings.Join(parts[:len(parts)-1], ":")
		port, _ = strconv.Atoi(parts[len(parts)-1])
	} else {
		host = parts[0]
	}

	addr, err := addr.Extract(host)
	if err != nil {
		return err
	}

	// register service
	node := &registry.Node{
		Id:       config.Name + "@" + config.ID,
		Address:  addr,
		Port:     port,
		Metadata: config.Metadata,
	}
	s.id = node.Id
	node.Metadata["server"] = s.String()
	node.Metadata["registry"] = config.Registry.String()

	s.RLock()
	// Maps are ordered randomly, sort the keys for consistency

	var endpoints []*registry.Endpoint

	s.RUnlock()

	service := &registry.Service{
		Name:      config.Name,
		Version:   config.Version,
		Nodes:     []*registry.Node{node},
		Endpoints: endpoints,
	}

	s.Lock()
	registered := s.registered
	s.Unlock()

	if !registered {
		log.Info("Registering node: %s", node.Id)
	}

	// create registry options
	rOpts := []registry.RegisterOption{registry.RegisterTTL(config.RegisterTTL)}

	if err := config.Registry.Register(service, rOpts...); err != nil {
		return err
	}

	// already registered? don't need to register subscribers
	if registered {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	s.registered = true

	return nil
}

// ServiceRegister 向Registry注销自己
func (s *server) ServiceDeregister() error {
	config := s.Options()
	var advt, host string
	var port int

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	parts := strings.Split(advt, ":")
	if len(parts) > 1 {
		host = strings.Join(parts[:len(parts)-1], ":")
		port, _ = strconv.Atoi(parts[len(parts)-1])
	} else {
		host = parts[0]
	}

	addr, err := addr.Extract(host)
	if err != nil {
		return err
	}

	node := &registry.Node{
		Id:      config.Name + "@" + config.ID,
		Address: addr,
		Port:    port,
	}

	service := &registry.Service{
		Name:    config.Name,
		Version: config.Version,
		Nodes:   []*registry.Node{node},
	}

	log.Info("Deregistering node: %s", node.Id)
	if err := config.Registry.Deregister(service); err != nil {
		return err
	}

	s.Lock()

	if !s.registered {
		s.Unlock()
		return nil
	}

	s.registered = false

	s.Unlock()
	return nil
}

func (s *server) Start() error {
	//config := s.Options()

	//s.Lock()
	// swap address
	//addr := s.opts.Address
	//s.opts.Address = ts.Addr()
	//s.Unlock()
	return nil
}

func (s *server) Stop() error {
	if s.server != nil {
		log.Info("RPCServer closeing id(%s)", s.id)
		err := s.server.Done()
		if err != nil {
			log.Warning("RPCServer close fail id(%s) error(%s)", s.id, err)
		} else {
			log.Info("RPCServer close success id(%s)", s.id)
		}
		s.server = nil
	}
	return nil
}

func (s *server) String() string {
	return "rpc"
}
