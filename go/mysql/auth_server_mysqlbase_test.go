/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/
// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

var foobarPwdSHA2Hash, _ = hex.DecodeString("24412430303524031A69251C34295C4B35167C7F1E5A7B63091349503974624D34504B5A424679354856336868686F52485A736E4A733368786E427575516C73446469496537")

// The output from NewHashPassword is not stable as the hash is based on the generated salt.
// This is why CheckHashingPassword is used here.
func TestNewSha2Password(t *testing.T) {
	pwd := "foobar"
	//pwhash := "$A$005${=Jd`a<;~ad%}^\"3X#XnYdfMUQcjRyMBUrXv4gZFFXjR5sAChERO1eZ9B99rD"
	pwhash1 := NewHashPassword(pwd, string(foobarPwdSHA2Hash))
	//r, err := CheckHashingPassword([]byte(pwhash), pwd, mysql.AuthCachingSha2Password)
	require.Equal(t, pwhash1, string(foobarPwdSHA2Hash))
	//for r := range pwhash {
	//	require.Less(t, pwhash[r], uint8(128))
	//	require.NotEqual(t, pwhash[r], 0)  // NUL
	//	require.NotEqual(t, pwhash[r], 36) // '$'
	//}
}
func TestGetUser(t *testing.T) {
	a := NewAuthServerMysqlBase()
	a.reLoadUser()
}
