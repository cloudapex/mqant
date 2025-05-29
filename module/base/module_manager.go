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

// Package basemodule  模块管理器
package modulebase

import (
	"fmt"
	"sync"

	"github.com/liangdas/mqant/conf"
	"github.com/liangdas/mqant/log"
	"github.com/liangdas/mqant/module"
	"github.com/liangdas/mqant/mqtools"
)

// NewModuleManager 新建模块管理器
func NewModuleManager() *ModuleManager {
	return &ModuleManager{}
}

// ModuleManager 模块管理器
type ModuleManager struct {
	app     module.IApp
	mods    []*moduleUnit
	runMods []*moduleUnit // 真正运行的modules
}

// moduleUnit 模块结构
type moduleUnit struct {
	mi       module.IModule
	settings *conf.ModuleSettings // from Config.Settings
	closeSig chan bool
	wg       sync.WaitGroup
}

// Register 注册模块
func (this *ModuleManager) Register(mi module.IModule) {
	md := new(moduleUnit)
	md.mi = mi
	md.closeSig = make(chan bool, 1)

	this.mods = append(this.mods, md)
}

// RegisterRunMod 注册需要运行的模块
func (this *ModuleManager) RegisterRunMod(mi module.IModule) {
	md := new(moduleUnit)
	md.mi = mi
	md.closeSig = make(chan bool, 1)

	this.runMods = append(this.runMods, md)
}

// Init 初始化
func (this *ModuleManager) Init(app module.IApp, processEnv string) {
	log.Info("This server process run ProcessEnvGroup is [%s]", processEnv)
	this.app = app
	this.CheckModuleSettings() // 配置文件规则检查(没通过的话直接panic)
	for i := 0; i < len(this.mods); i++ {
		// 代码中注册的module到配置中匹配
		for typ, modSettings := range app.Config().Module {
			if this.mods[i].mi.GetType() == typ { // this.mods[i]匹配Conf.ModuleType
				for _, setting := range modSettings {
					// 这里可能有BUG 公网IP和局域网IP处理方式可能不一样,先不管
					if processEnv == setting.ProcessEnv { // 有匹配到
						this.runMods = append(this.runMods, this.mods[i]) // 加入到运行列表中
						this.mods[i].settings = setting
					}
				}
				break //跳出内部循环
			}
		}
	}

	for i := 0; i < len(this.runMods); i++ {
		m := this.runMods[i]
		m.mi.OnInit(app, m.settings)

		if app.GetModuleInited() != nil {
			app.GetModuleInited()(app, m.mi)
		}

		m.wg.Add(1)
		go func(unit *moduleUnit) {
			defer func() {
				if err := mqtools.Catch(recover()); err != nil {
					log.Error("module[%q] run panic: %v", unit.mi.GetType(), err)
				}
			}()
			unit.mi.Run(unit.closeSig)
			unit.wg.Done()
		}(m)
	}
	//timer.SetTimer(3, this.ReportStatistics, nil) //统计汇报定时任务
}

// CheckModuleSettings module配置文件规则检查(ID全局必须唯一) 且 每一个类型的Module列表中ProcessID不能重复
func (this *ModuleManager) CheckModuleSettings() {
	gid := map[string]string{} // 用来保存全局ID-ModuleType
	for typ, modSettings := range this.app.Config().Module {
		pid := map[string]string{} //用来保存模块中的 ProcessID-ID
		for _, setting := range modSettings {
			if Stype, ok := gid[setting.ID]; ok {
				//如果Id已经存在,说明有两个相同Id的模块,这种情况不能被允许,这里就直接抛异常 强制崩溃以免以后调试找不到问题
				panic(fmt.Sprintf("ID (%s) been used in modules of type [%s] and cannot be reused", setting.ID, Stype))
			} else {
				gid[setting.ID] = typ
			}

			if id, ok := pid[setting.ProcessEnv]; ok {
				//如果ProcessID已经存在,说明有两个相同ProcessID的模块,这种情况不能被允许,这里就直接抛异常 强制崩溃以免以后调试找不到问题
				panic(fmt.Sprintf("In the list of modules of type [%s], ProcessID (%s) has been used for ID module for (%s)", typ, setting.ProcessEnv, id))
			} else {
				pid[setting.ProcessEnv] = setting.ID
			}
		}
	}
}

// Destroy 停止模块(倒序)
func (this *ModuleManager) Destroy() {
	for i := len(this.runMods) - 1; i >= 0; i-- {
		m := this.runMods[i]
		m.closeSig <- true
		m.wg.Wait()
		func(unit *moduleUnit) {
			defer func() {
				if err := mqtools.Catch(recover()); err != nil {
					log.Error("module[%q] destroy panic: %v", unit.mi.GetType(), err)
				}
			}()
			unit.mi.OnDestroy()
		}(m)
	}
}
