/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2020 The Vitess Authors.

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

package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/wesql/wescale/go/vt/key"
	querypb "github.com/wesql/wescale/go/vt/proto/query"
)

func TestSendTableUnShardWithoutKS(t *testing.T) {
	type testCase struct {
		testName         string
		destination      key.Destination
		expectedQueryLog []string
		expectedError    string
		isDML            bool
	}

	tests := []testCase{
		{
			testName:    "unsharded with no autocommit",
			destination: key.DestinationShard("0"),
			expectedQueryLog: []string{
				`ResolveDestinations without keyspace DestinationShard(0)`,
				`ExecuteMultiShard .DestinationShard(0): dummy_query {} false false`,
			},
			isDML: false,
		},
		{
			testName:    "unsharded",
			destination: key.DestinationShard("0"),
			expectedQueryLog: []string{
				`ResolveDestinations without keyspace DestinationShard(0)`,
				`ExecuteMultiShard .DestinationShard(0): dummy_query {} true true`,
			},
			isDML: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			send := &Send{
				Keyspace:          nil,
				Query:             "dummy_query",
				TargetDestination: tc.destination,
				IsDML:             tc.isDML,
				SingleShardOnly:   false,
			}
			vc := &loggingVCursor{shards: []string{"0"}}
			_, err := send.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			vc.ExpectLog(t, tc.expectedQueryLog)

			// Failure cases
			vc = &loggingVCursor{shardErr: errors.New("shard_error")}
			_, err = send.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
			require.EqualError(t, err, "shard_error")
		})
	}
}

func TestSendTableUnShardWithoutKS_StreamExecute(t *testing.T) {
	type testCase struct {
		testName         string
		destination      key.Destination
		expectedQueryLog []string
		expectedError    string
		isDML            bool
	}

	tests := []testCase{
		{
			testName:    "unsharded with no autocommit",
			destination: key.DestinationShard("0"),
			expectedQueryLog: []string{
				`ResolveDestinations without keyspace DestinationShard(0)`,
				`StreamExecuteMulti dummy_query .DestinationShard(0): {} `,
			},
			isDML: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			send := &Send{
				Keyspace:          nil,
				Query:             "dummy_query",
				TargetDestination: tc.destination,
				IsDML:             tc.isDML,
				SingleShardOnly:   false,
			}
			vc := &loggingVCursor{shards: []string{"0"}}
			_, err := wrapStreamExecute(send, vc, map[string]*querypb.BindVariable{}, false)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			vc.ExpectLog(t, tc.expectedQueryLog)
		})
	}
}
