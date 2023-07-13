/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package mysqlbasedacl

import (
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/tableacl/acl"
)

// MysqlBasedACL keeps all entries from mysql table in a unique in-memory list
type MysqlBasedACL map[string]bool

func BuildMysqlBasedACLKey(username, host string) string {
	if host == "" {
		return username
	}
	return fmt.Sprintf("%s@%s", username, host)
}

// IsMember checks the membership of a principal in this ACL
func (sacl MysqlBasedACL) IsMember(principal *querypb.VTGateCallerID) bool {
	key := BuildMysqlBasedACLKey(principal.GetUsername(), principal.GetHost())
	if sacl[key] {
		return true
	}
	for _, grp := range principal.Groups {
		if sacl[grp] {
			return true
		}
	}
	return false
}

// Factory is responsible to create new ACL instance.
type Factory struct{}

// New creates a new ACL instance.
// entries : {'root'@'localhost','user'@'192.168.1.1'}
func (factory *Factory) New(entries []string) (acl.ACL, error) {
	acl := MysqlBasedACL(map[string]bool{})
	for _, e := range entries {
		acl[e] = true
	}
	return acl, nil
}
