/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsDirect(t *testing.T) {
	assert.True(t, DDLStrategyDirect.IsDirect())
	assert.False(t, DDLStrategyVitess.IsDirect())
	assert.False(t, DDLStrategyOnline.IsDirect())
	assert.True(t, DDLStrategy("").IsDirect())
	assert.False(t, DDLStrategy("vitess").IsDirect())
	assert.False(t, DDLStrategy("online").IsDirect())
	assert.False(t, DDLStrategy("mysql").IsDirect())
	assert.True(t, DDLStrategy("something").IsDirect())
}

func TestParseDDLStrategy(t *testing.T) {
	tt := []struct {
		strategyVariable     string
		strategy             DDLStrategy
		options              string
		isSingleton          bool
		isPostponeLaunch     bool
		isPostponeCompletion bool
		isInOrderCompletion  bool
		isAllowConcurrent    bool
		fastOverRevertible   bool
		fastRangeRotation    bool
		allowForeignKeys     bool
		runtimeOptions       string
		err                  error
	}{
		{
			strategyVariable: "direct",
			strategy:         DDLStrategyDirect,
		},
		{
			strategyVariable: "vitess",
			strategy:         DDLStrategyVitess,
		},
		{
			strategyVariable: "online",
			strategy:         DDLStrategyOnline,
		},
		{
			strategyVariable: "mysql",
			strategy:         DDLStrategyMySQL,
		},
		{
			strategy: DDLStrategyDirect,
		},
		{
			strategyVariable: "online -postpone-launch",
			strategy:         DDLStrategyOnline,
			options:          "-postpone-launch",
			runtimeOptions:   "",
			isPostponeLaunch: true,
		},
		{
			strategyVariable:     "online -postpone-completion",
			strategy:             DDLStrategyOnline,
			options:              "-postpone-completion",
			runtimeOptions:       "",
			isPostponeCompletion: true,
		},
		{
			strategyVariable:    "online --in-order-completion",
			strategy:            DDLStrategyOnline,
			options:             "--in-order-completion",
			runtimeOptions:      "",
			isInOrderCompletion: true,
		},
		{
			strategyVariable:  "online -allow-concurrent",
			strategy:          DDLStrategyOnline,
			options:           "-allow-concurrent",
			runtimeOptions:    "",
			isAllowConcurrent: true,
		},
		{
			strategyVariable:  "vitess -allow-concurrent",
			strategy:          DDLStrategyVitess,
			options:           "-allow-concurrent",
			runtimeOptions:    "",
			isAllowConcurrent: true,
		},
		{
			strategyVariable:   "vitess --prefer-instant-ddl",
			strategy:           DDLStrategyVitess,
			options:            "--prefer-instant-ddl",
			runtimeOptions:     "",
			fastOverRevertible: true,
		},
		{
			strategyVariable:  "vitess --fast-range-rotation",
			strategy:          DDLStrategyVitess,
			options:           "--fast-range-rotation",
			runtimeOptions:    "",
			fastRangeRotation: true,
		},
		{
			strategyVariable: "vitess --unsafe-allow-foreign-keys",
			strategy:         DDLStrategyVitess,
			options:          "--unsafe-allow-foreign-keys",
			runtimeOptions:   "",
			allowForeignKeys: true,
		},
	}
	for _, ts := range tt {
		t.Run(ts.strategyVariable, func(t *testing.T) {
			setting, err := ParseDDLStrategy(ts.strategyVariable)
			assert.NoError(t, err)
			assert.Equal(t, ts.strategy, setting.Strategy)
			assert.Equal(t, ts.options, setting.Options)
			assert.Equal(t, ts.isSingleton, setting.IsSingleton())
			assert.Equal(t, ts.isPostponeCompletion, setting.IsPostponeCompletion())
			assert.Equal(t, ts.isPostponeLaunch, setting.IsPostponeLaunch())
			assert.Equal(t, ts.isAllowConcurrent, setting.IsAllowConcurrent())
			assert.Equal(t, ts.fastOverRevertible, setting.IsPreferInstantDDL())
			assert.Equal(t, ts.fastRangeRotation, setting.IsFastRangeRotationFlag())
			assert.Equal(t, ts.allowForeignKeys, setting.IsAllowForeignKeysFlag())

			runtimeOptions := strings.Join(setting.RuntimeOptions(), " ")
			assert.Equal(t, ts.runtimeOptions, runtimeOptions)
		})
	}
	{
		_, err := ParseDDLStrategy("other")
		assert.Error(t, err)
	}
}
