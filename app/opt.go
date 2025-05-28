package app

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/selector/cache"
)

// 启动参数(命令行优先级比环境变量高)
type startUpArgs struct {
	WordDir    string `env:"wd" env-default:""`
	ProcessEnv string `env:"env" env-default:"dev"`
	ConsulAddr string `env:"consul" env-default:"127.0.0.1:8500"` // configKey: config/{env}/server
	LogPath    string `env:"log" env-default:""`
	BiPath     string `env:"bi" env-default:""`
	PProfAddr  string `env:"pprof" env-default:""`
}

func newOptions(opts ...module.Option) module.Options {

	// default value
	opt := module.Options{
		Version:          "1.0.0",
		Selector:         cache.NewSelector(), // 这两个
		RegisterInterval: time.Second * time.Duration(10),
		RegisterTTL:      time.Second * time.Duration(20),
		KillWaitTTL:      time.Second * time.Duration(60),
		RPCExpired:       time.Second * time.Duration(10),
		RPCMaxCoroutine:  0, //不限制
		Debug:            true,
		Parse:            true,
		LogFileName: func(logdir, prefix, processID, suffix string) string {
			return fmt.Sprintf("%s/%v%s%s", logdir, prefix, processID, suffix)
		},
		BIFileName: func(logdir, prefix, processID, suffix string) string {
			return fmt.Sprintf("%s/%v%s%s", logdir, prefix, processID, suffix)
		},
	}

	for _, o := range opts {
		o(&opt)
	}

	// 解析启动参数
	var startArgs = startUpArgs{}
	if opt.Parse {
		// 其次使用环境变量
		if err := cleanenv.ReadEnv(&startArgs); err != nil {
			panic(err)
		}
		// 优先使用命令行参数
		flag.StringVar(&startArgs.WordDir, "wd", startArgs.WordDir, "Server work directory")
		flag.StringVar(&startArgs.ProcessEnv, "env", startArgs.ProcessEnv, "Server ProcessEnv")
		flag.StringVar(&startArgs.ConsulAddr, "consul", startArgs.ConsulAddr, "Consul server addr")
		flag.StringVar(&startArgs.LogPath, "log", startArgs.LogPath, "Log file directory")
		flag.StringVar(&startArgs.BiPath, "bi", startArgs.BiPath, "bi file directory")
		flag.StringVar(&startArgs.PProfAddr, "pprof", startArgs.PProfAddr, "listen pprof addr")
		flag.Parse()
	}

	// 工作目录(优先使用代码中的)
	if opt.WorkDir == "" {
		opt.WorkDir = startArgs.WordDir
	}

	// 设置进程分组环境(优先使用代码中的)
	if opt.ProcessEnv == "" {
		opt.ProcessEnv = startArgs.ProcessEnv
		if opt.ProcessEnv == "" {
			opt.ProcessEnv = "dev"
		}
	}

	// 最终检查设置工作目录
	ApplicationDir := ""
	if opt.WorkDir != "" {
		_, err := os.Open(opt.WorkDir)
		if err != nil {
			panic(err)
		}
		os.Chdir(opt.WorkDir)
		ApplicationDir, err = os.Getwd()
	} else {
		var err error
		ApplicationDir, err = os.Getwd()
		if err != nil {
			file, _ := exec.LookPath(os.Args[0])
			ApplicationPath, _ := filepath.Abs(file)
			ApplicationDir, _ = filepath.Split(ApplicationPath)
		}

	}
	opt.WorkDir = ApplicationDir

	// nats addr
	if len(opt.ConsulAddr) == 0 {
		opt.ConsulAddr = append(opt.ConsulAddr, startArgs.ConsulAddr)
	}

	// configkey
	if opt.ConfigKey == "" {
		opt.ConfigKey = fmt.Sprintf("config/%v/server", startArgs.ProcessEnv)
	}

	// 创建日志文件
	defaultLogPath := fmt.Sprintf("%s/bin/logs", ApplicationDir)
	defaultBIPath := fmt.Sprintf("%s/bin/bi", ApplicationDir)

	if opt.LogDir == "" { // 优先使用代码中的
		opt.LogDir = defaultLogPath
		if startArgs.LogPath != "" {
			opt.LogDir = startArgs.LogPath
		}
	}
	if opt.BIDir == "" { // 优先使用代码中的
		opt.BIDir = defaultBIPath
		if startArgs.BiPath != "" {
			opt.BIDir = startArgs.BiPath
		}
	}

	if _, err := os.Stat(opt.LogDir); os.IsNotExist(err) {
		if err := os.Mkdir(opt.LogDir, os.ModePerm); err != nil {
			fmt.Println(err)
		}
	}
	if _, err := os.Stat(opt.BIDir); os.IsNotExist(err) {
		if err := os.Mkdir(opt.BIDir, os.ModePerm); err != nil {
			fmt.Println(err)
		}
	}

	// pprof
	opt.PProfAddr = startArgs.PProfAddr
	if opt.PProfAddr != "" {
		go func() {
			err := http.ListenAndServe(opt.PProfAddr, nil)
			fmt.Printf("StartPProf err:%v \n", err)
		}()
	}

	return opt
}
