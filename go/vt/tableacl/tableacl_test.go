/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

/*
Copyright 2019 The Vitess Authors.

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

package tableacl

import (
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"vitess.io/vitess/go/vt/tableacl/mysqlbasedacl"

	"vitess.io/vitess/go/internal/global"

	"vitess.io/vitess/go/vt/dbconfigs"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/tableacl/acl"
	"vitess.io/vitess/go/vt/tableacl/simpleacl"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
)

type fakeACLFactory struct{}

func (factory *fakeACLFactory) New(entries []string) (acl.ACL, error) {
	return nil, errors.New("unable to create a new ACL")
}

func TestInitWithInvalidFilePath(t *testing.T) {
	tacl := TableACL{factory: &simpleacl.Factory{}}
	if err := tacl.init(nil, dbconfigs.New(nil), global.TableACLModeSimple, "/invalid_file_path", 0, func() {}); err == nil {
		t.Fatalf("init should fail for an invalid config file path")
	}
}

var aclJSON = `{
  "table_groups": [
    {
      "name": "group01",
      "table_names_or_prefixes": ["test_table"],
      "readers": ["vt"],
      "writers": ["vt"]
    }
  ]
}`

func TestInitWithValidConfig(t *testing.T) {
	tacl := TableACL{factory: &simpleacl.Factory{}}
	f, err := os.CreateTemp("", "tableacl")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := io.WriteString(f, aclJSON); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tacl.init(nil, dbconfigs.New(nil), global.TableACLModeSimple, f.Name(), 0, func() {}); err != nil {
		t.Fatal(err)
	}
}

func TestInitFromProto(t *testing.T) {
	tacl := TableACL{factory: &simpleacl.Factory{}}
	readerACL := tacl.Authorized("my_test_table", READER)
	want := &ACLResult{ACL: acl.DenyAllACL{}, GroupName: ""}
	if !reflect.DeepEqual(readerACL, want) {
		t.Fatalf("tableacl has not been initialized, got: %v, want: %v", readerACL, want)
	}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
		}},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("tableacl init should succeed, but got error: %v", err)
	}
	if got := tacl.Config(); !proto.Equal(got, config) {
		t.Fatalf("GetCurrentConfig() = %v, want: %v", got, config)
	}
	readerACL = tacl.Authorized("unknown_table", READER)
	if !reflect.DeepEqual(readerACL, want) {
		t.Fatalf("there is no config for unknown_table, should deny by default")
	}
	readerACL = tacl.Authorized("test_table", READER)
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "vt"}) {
		t.Fatalf("user: vt should have reader permission to table: test_table")
	}
}

func TestTableACLValidateConfig(t *testing.T) {
	tests := []struct {
		names []string
		valid bool
	}{
		{nil, true},
		{[]string{}, true},
		{[]string{"b"}, true},
		{[]string{"b", "a"}, true},
		{[]string{"b%c"}, false},                    // invalid entry
		{[]string{"aaa", "aaab%", "aaabb"}, false},  // overlapping
		{[]string{"aaa", "aaab", "aaab%"}, false},   // overlapping
		{[]string{"a", "aa%", "aaab%"}, false},      // overlapping
		{[]string{"a", "aa%", "aaab"}, false},       // overlapping
		{[]string{"a", "aa", "aaa%%"}, false},       // invalid entry
		{[]string{"a", "aa", "aa", "aaaaa"}, false}, // duplicate
	}
	for _, test := range tests {
		var groups []*tableaclpb.TableGroupSpec
		for _, name := range test.names {
			groups = append(groups, &tableaclpb.TableGroupSpec{
				TableNamesOrPrefixes: []string{name},
			})
		}
		config := &tableaclpb.Config{TableGroups: groups}
		err := ValidateProto(config)
		if test.valid && err != nil {
			t.Fatalf("ValidateProto(%v) = %v, want nil", config, err)
		} else if !test.valid && err == nil {
			t.Fatalf("ValidateProto(%v) = nil, want error", config)
		}
	}
}

func TestTableACLAuthorize(t *testing.T) {
	tacl := TableACL{factory: &simpleacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"test_music"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
			{
				Name:                 "group02",
				TableNamesOrPrefixes: []string{"test_music_02", "test_video"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u3"},
				Admins:               []string{"u4"},
			},
			{
				Name:                 "group03",
				TableNamesOrPrefixes: []string{"test_other%"},
				Readers:              []string{"u2"},
				Writers:              []string{"u2", "u3"},
				Admins:               []string{"u3"},
			},
			{
				Name:                 "group04",
				TableNamesOrPrefixes: []string{"test_data%"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	readerACL := tacl.Authorized("test_data_any", READER)
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "u1"}) {
		t.Fatalf("user u1 should have reader permission to table test_data_any")
	}
	if !readerACL.IsMember(&querypb.VTGateCallerID{Username: "u2"}) {
		t.Fatalf("user u2 should have reader permission to table test_data_any")
	}
}

func TestFailedToCreateACL(t *testing.T) {
	tacl := TableACL{factory: &fakeACLFactory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{{
			Name:                 "group01",
			TableNamesOrPrefixes: []string{"test_table"},
			Readers:              []string{"vt"},
			Writers:              []string{"vt"},
		}},
	}
	if err := tacl.Set(config); err == nil {
		t.Fatalf("tableacl init should fail because fake ACL returns an error")
	}
}

func TestDoubleRegisterTheSameKey(t *testing.T) {
	name := "tableacl-name-TestDoubleRegisterTheSameKey"
	Register(name, &simpleacl.Factory{})
	defer func() {
		err := recover()
		if err == nil {
			t.Fatalf("the second tableacl register should fail")
		}
	}()
	Register(name, &simpleacl.Factory{})
}

func TestGetCurrentAclFactory(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactory"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	f, err := GetCurrentACLFactory()
	if err != nil {
		t.Errorf("Fail to get current ACL Factory: %v", err)
	}
	if !reflect.DeepEqual(aclFactory, f) {
		t.Fatalf("should return registered acl factory even if default acl is not set.")
	}
	Register(name+"-2", aclFactory)
	_, err = GetCurrentACLFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default is not set")
	}
}

func TestGetCurrentACLFactoryWithWrongDefault(t *testing.T) {
	acls = make(map[string]acl.Factory)
	defaultACL = ""
	name := "tableacl-name-TestGetCurrentAclFactoryWithWrongDefault"
	aclFactory := &simpleacl.Factory{}
	Register(name+"-1", aclFactory)
	Register(name+"-2", aclFactory)
	SetDefaultACL("wrong_name")
	_, err := GetCurrentACLFactory()
	if err == nil {
		t.Fatalf("there are more than one acl factories, but the default given does not match any of these.")
	}
}

func IsMemberInList(name string, acls []*ACLResult) bool {
	for _, acl := range acls {
		if acl.IsMember(&querypb.VTGateCallerID{Username: name}) {
			return true
		}
	}
	return false
}

func TestAuthorizedList(t *testing.T) {
	tacl := TableACL{factory: &mysqlbasedacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"%"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	readerACL := tacl.AuthorizedList("test_data_any", READER)
	if !IsMemberInList("u1", readerACL) && !IsMemberInList("u2", readerACL) {
		t.Fatalf("user u1 should have reader permission to table test_data_any")
	}
	if IsMemberInList("u3", readerACL) {
		t.Fatalf("user u3 should not have reader permission to table test_data_any")
	}
	wriderACL := tacl.AuthorizedList("test_data_any", WRITER)
	if !IsMemberInList("u1", wriderACL) && !IsMemberInList("u3", wriderACL) {
		t.Fatalf("user u1 should have writer permission to table test_data_any")
	}
	if IsMemberInList("u2", wriderACL) {
		t.Fatalf("user u3 should not have writer permission to table test_data_any")
	}
	adminsACL := tacl.AuthorizedList("test_data_any", ADMIN)
	if !IsMemberInList("u1", adminsACL) {
		t.Fatalf("user u1 should have writer permission to table test_data_any")
	}
	if IsMemberInList("u3", adminsACL) {
		t.Fatalf("user u3 should not have writer permission to table test_data_any")
	}
}

func TestAuthorizedListNoMatch(t *testing.T) {
	tacl := TableACL{factory: &mysqlbasedacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"table1"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	readerACL := tacl.AuthorizedList("non_existent_table", READER)
	if len(readerACL) > 0 {
		t.Fatalf("Non existent table should not have any permissions")
	}
}

func TestAuthorizedListPartialMatch(t *testing.T) {
	tacl := TableACL{factory: &mysqlbasedacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"test_data_%"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	readerACL := tacl.AuthorizedList("test_data_any", READER)
	if !IsMemberInList("u1", readerACL) && !IsMemberInList("u2", readerACL) {
		t.Fatalf("user u1 and u2 should have reader permission to table test_data_any")
	}

	readerACL = tacl.AuthorizedList("other_table", READER)
	if len(readerACL) > 0 {
		t.Fatalf("other_table does not match the prefix test_data_%%, no permissions should be granted")
	}
}

func TestAuthorizedListEmptyACL(t *testing.T) {
	tacl := TableACL{factory: &mysqlbasedacl.Factory{}}

	readerACL := tacl.AuthorizedList("test_data_any", READER)
	if len(readerACL) > 0 {
		t.Fatalf("No ACLs have been set, no permissions should be granted")
	}
}

func TestAuthorizedListDatabaseWildcard(t *testing.T) {
	tacl := TableACL{factory: &mysqlbasedacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"database.%"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	// Validating access to a specific table in the "database" database.
	readerACL := tacl.AuthorizedList("database.table1", READER)
	if !IsMemberInList("u1", readerACL) && !IsMemberInList("u2", readerACL) {
		t.Fatalf("user u1 and u2 should have reader permission to table database.table1")
	}
}

func TestAuthorizedListSpecificTable(t *testing.T) {
	tacl := TableACL{factory: &mysqlbasedacl.Factory{}}
	config := &tableaclpb.Config{
		TableGroups: []*tableaclpb.TableGroupSpec{
			{
				Name:                 "group01",
				TableNamesOrPrefixes: []string{"database.table"},
				Readers:              []string{"u1", "u2"},
				Writers:              []string{"u1", "u3"},
				Admins:               []string{"u1"},
			},
		},
	}
	if err := tacl.Set(config); err != nil {
		t.Fatalf("InitFromProto(<data>) = %v, want: nil", err)
	}

	// Validating access to a specific table.
	readerACL := tacl.AuthorizedList("database.table", READER)
	if !IsMemberInList("u1", readerACL) && !IsMemberInList("u2", readerACL) {
		t.Fatalf("user u1 and u2 should have reader permission to table database.table")
	}

	// Validating no access to other tables in the same database.
	readerACL = tacl.AuthorizedList("database.other_table", READER)
	if len(readerACL) > 0 {
		t.Fatalf("user should not have access to table database.other_table")
	}
}
