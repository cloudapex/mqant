package app

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/registry"
	"github.com/liangdas/mqant/selector/cache"
	"github.com/nats-io/nats.go"
)

func newOptions(opts ...module.Option) module.Options {
	var wdPath, confPath, Logdir, BIdir *string
	var ProcessEnv *string
	opt := module.Options{
		Registry:         registry.DefaultRegistry,
		Selector:         cache.NewSelector(),
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

	// 解析输入的参数
	if opt.Parse {
		wdPath = flag.String("wd", "", "Server work directory")
		confPath = flag.String("conf", "", "Server configuration file path")
		ProcessEnv = flag.String("env", "development", "Server ProcessEnv?")
		Logdir = flag.String("log", "", "Log file directory?")
		BIdir = flag.String("bi", "", "bi file directory?")
		flag.Parse()
	}

	if opt.Nats == nil {
		nc, err := nats.Connect(nats.DefaultURL)
		if err != nil {
			log.Error("nats agent: %s", err.Error())
		}
		opt.Nats = nc
	}

	if opt.WorkDir == "" {
		opt.WorkDir = *wdPath
	}

	// 设置进程分组环境
	if opt.ProcessEnv == "" {
		opt.ProcessEnv = *ProcessEnv
		if opt.ProcessEnv == "" {
			opt.ProcessEnv = "development"
		}
	}

	// 检查设置工作目录
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

	// 检查主配置文件和创建日志文件
	defaultConfPath := fmt.Sprintf("%s/bin/conf/server.json", ApplicationDir)
	defaultLogPath := fmt.Sprintf("%s/bin/logs", ApplicationDir)
	defaultBIPath := fmt.Sprintf("%s/bin/bi", ApplicationDir)

	if opt.ConfPath == "" {
		if *confPath == "" {
			opt.ConfPath = defaultConfPath
		} else {
			opt.ConfPath = *confPath
		}
	}

	if opt.LogDir == "" {
		if *Logdir == "" {
			opt.LogDir = defaultLogPath
		} else {
			opt.LogDir = *Logdir
		}
	}

	if opt.BIDir == "" {
		if *BIdir == "" {
			opt.BIDir = defaultBIPath
		} else {
			opt.BIDir = *BIdir
		}
	}

	if _, err := os.Stat(opt.ConfPath); os.IsNotExist(err) {
		panic(fmt.Sprintf("config path error %v", err))
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
	return opt
}
