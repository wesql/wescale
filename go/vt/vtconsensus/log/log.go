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

package log

import (
	"fmt"

	"vitess.io/vitess/go/vt/log"
)

// Logger is a wrapper that prefix loglines with keyspace/shard
type Logger struct {
	prefix string
}

// NewVTConsensusLogger creates a new logger
func NewVTConsensusLogger(keyspace, shard string) *Logger {
	return &Logger{
		prefix: fmt.Sprintf("%s/%s", keyspace, shard),
	}
}

// Info formats arguments like fmt.Print
func (logger *Logger) Info(msg string) {
	log.InfoDepth(1, logger.annotate(msg))
}

// Infof formats arguments like fmt.Printf.
func (logger *Logger) Infof(format string, args ...any) {
	log.InfoDepth(1, logger.annotate(fmt.Sprintf(format, args...)))
}

// Warning formats arguments like fmt.Print
func (logger *Logger) Warning(msg string) {
	log.WarningDepth(1, logger.annotate(msg))
}

// Warningf formats arguments like fmt.Printf.
func (logger *Logger) Warningf(format string, args ...any) {
	log.WarningDepth(1, logger.annotate(fmt.Sprintf(format, args...)))
}

// Error formats arguments like fmt.Print
func (logger *Logger) Error(msg string) {
	log.ErrorDepth(1, logger.annotate(msg))
}

// Errorf formats arguments like fmt.Printf.
func (logger *Logger) Errorf(format string, args ...any) {
	log.ErrorDepth(1, logger.annotate(fmt.Sprintf(format, args...)))
}

func (logger *Logger) annotate(input string) string {
	return fmt.Sprintf("shard=%s %s", logger.prefix, input)
}
