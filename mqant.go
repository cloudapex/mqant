// Copyright 2017 mqant Author. All Rights Reserved.
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

// Package mqant mqant
package mqant

import (
	"github.com/liangdas/mqant/app"
	"github.com/liangdas/mqant/module"
)

var App module.IApp = nil

// CreateApp 创建mqant的app实例
func CreateApp(opts ...module.Option) module.IApp {
	App = app.NewApp(append(opts, module.Version(version))...)
	return App

}
