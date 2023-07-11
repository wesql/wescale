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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/internal/global"

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"github.com/tchap/go-patricia/patricia"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/tableacl/acl"

	tableaclpb "vitess.io/vitess/go/vt/proto/tableacl"
)

// ACLResult embeds an acl.ACL and also tell which table group it belongs to.
type ACLResult struct {
	acl.ACL
	GroupName string
}

type aclEntry struct {
	tableNameOrPrefix string
	groupName         string
	acl               map[Role]acl.ACL
}

type aclEntries []aclEntry

func (aes aclEntries) Len() int {
	return len(aes)
}

func (aes aclEntries) Less(i, j int) bool {
	return aes[i].tableNameOrPrefix < aes[j].tableNameOrPrefix
}

func (aes aclEntries) Swap(i, j int) {
	aes[i], aes[j] = aes[j], aes[i]
}

// mu protects acls and defaultACL.
var mu sync.Mutex

var acls = make(map[string]acl.Factory)

// defaultACL tells the default ACL implementation to use.
var defaultACL string

type tableACL struct {
	// mutex protects entries, config, and callback
	sync.RWMutex
	entries aclEntries
	config  *tableaclpb.Config

	dbConfig dbconfigs.Connector
	conns    *connpool.Pool

	// callback is executed on successful reload.
	callback func()
	// ACL Factory override for testing
	factory acl.Factory
}

type PrivEntry struct {
	User string
	role []Role
}

// currentTableACL stores current effective ACL information.
var currentTableACL tableACL

// Init initiates table ACLs.
//
// The config file can be binary-proto-encoded, or json-encoded.
// In the json case, it looks like this:
//
//	{
//	  "table_groups": [
//	    {
//	      "table_names_or_prefixes": ["name1"],
//	      "readers": ["client1"],
//	      "writers": ["client1"],
//	      "admins": ["client1"]
//	    }
//	  ]
//	}
func Init(env tabletenv.Env, dbConfig dbconfigs.Connector, tableACLMode string, configFile string, aclCB func()) error {
	return currentTableACL.init(env, dbConfig, tableACLMode, configFile, aclCB)
}

func (tacl *tableACL) InitMysqlBasedACL(env tabletenv.Env) error {
	config := &tableaclpb.Config{}
	pool := connpool.NewPool(env, "", tabletenv.ConnPoolConfig{
		Size:               3,
		IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
	})
	tacl.conns = pool
	tacl.conns.Open(tacl.dbConfig, tacl.dbConfig, tacl.dbConfig)
	return tacl.LoadFromMysql(config)
}
func (tacl *tableACL) InitSimpleACL(configFile string) error {
	config := &tableaclpb.Config{}
	data, err := os.ReadFile(configFile)
	if err != nil {
		log.Infof("unable to read tableACL config file: %v  Error: %v", configFile, err)
		return err
	}
	if err := proto.Unmarshal(data, config); err != nil {
		// try to parse tableacl as json file
		if jsonErr := json2.Unmarshal(data, config); jsonErr != nil {
			log.Infof("unable to parse tableACL config file as a protobuf or json file.  protobuf err: %v  json err: %v", err, jsonErr)
			return fmt.Errorf("unable to unmarshal Table ACL data: %s", data)
		}
	}
	return tacl.Set(config)
}

func (tacl *tableACL) init(env tabletenv.Env, dbConfig dbconfigs.Connector, tableACLMode string, configFile string, aclCB func()) error {
	tacl.SetCallback(aclCB)
	tacl.dbConfig = dbConfig
	if configFile == "" {
		return nil
	}
	if tableACLMode == global.TableACLModeMysqlBased {
		return tacl.InitMysqlBasedACL(env)
	} else if tableACLMode == global.TableACLModeSimple {
		return tacl.InitSimpleACL(configFile)
	} else {
		return fmt.Errorf("unrecognized tableACLMode : %v", tableACLMode)
	}
}

func (tacl *tableACL) SetCallback(callback func()) {
	tacl.Lock()
	defer tacl.Unlock()
	tacl.callback = callback
}
func BuildMysqlBasedACLKey(username, host string) string {
	return fmt.Sprintf("%s@%s", username, host)
}

// InitFromProto inits table ACLs from a proto.
func InitFromProto(config *tableaclpb.Config) error {
	return currentTableACL.Set(config)
}

func (tacl *tableACL) GetFromMysqlBase(newACL func([]string) (acl.ACL, error)) (aclEntries, error) {
	entries := aclEntries{}
	globalEntries, err := tacl.GetGlobalFromMysqlBase(newACL)
	if err != nil {
		return nil, err
	}
	entries = append(entries, globalEntries...)
	tableEntries, err := tacl.GetTablePrivFromMysqlBase(newACL)
	if err != nil {
		return nil, err
	}
	entries = append(entries, tableEntries...)
	dbEntries, err := tacl.GetDatabasePrivFromMysqlBase(newACL)
	if err != nil {
		return nil, err
	}
	entries = append(entries, dbEntries...)
	return entries, nil
}

func buildACLEntriesFromPrivMap(privMap map[string][]PrivEntry, newACL func([]string) (acl.ACL, error)) (aclEntries, error) {
	entries := aclEntries{}
	for key, privEntry := range privMap {
		var readerStrs []string
		var writerStrs []string
		var adminStrs []string
		for _, entry := range privEntry {
			for _, role := range entry.role {
				switch role {
				case READER:
					readerStrs = append(readerStrs, entry.User)
				case WRITER:
					writerStrs = append(writerStrs, entry.User)
				case ADMIN:
					adminStrs = append(adminStrs, entry.User)
				}
			}
		}
		readers, err := newACL(readerStrs)
		if err != nil {
			log.Infof("readers load from readerStrs fail")
			return nil, err
		}
		writers, err := newACL(writerStrs)
		if err != nil {
			log.Infof("writers load from writerStrs fail")
			return nil, err
		}
		admins, err := newACL(adminStrs)
		if err != nil {
			log.Infof("admins load from adminStrs fail")
			return nil, err
		}
		entries = append(entries, aclEntry{
			tableNameOrPrefix: key,
			groupName:         key,
			acl: map[Role]acl.ACL{
				READER: readers,
				WRITER: writers,
				ADMIN:  admins,
			},
		})
	}
	return entries, nil
}

func (tacl *tableACL) GetDatabasePrivFromMysqlBase(newACL func([]string) (acl.ACL, error)) (aclEntries, error) {
	ctx := context.Background()
	conn, err := tacl.conns.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	qr, err := conn.Exec(ctx, mysql.FetchDataBasePriv, 1000, false)
	if err != nil {
		log.Infof("loadFromMysqlBase fail %v", err)
	}
	isReader := func(privs []string) bool {
		return privs[0] == "Y"
	}
	isWriter := func(privs []string) bool {
		return privs[1] == "Y" && privs[2] == "Y" && privs[3] == "Y"
	}
	isAdmin := func(privs []string) bool {
		for _, flag := range privs {
			if flag == "N" {
				return false
			}
		}
		return true
	}
	privMap := make(map[string][]PrivEntry)
	for _, rows := range qr.Rows {
		user := rows[0].ToString()
		host := rows[1].ToString()
		database := rows[2].ToString()
		userKey := fmt.Sprintf("%s@%s", user, host)
		tableKey := fmt.Sprintf("%s.%%", database)
		privEntry := PrivEntry{
			User: userKey,
		}
		var dbPrivs []string
		for index := 3; index < len(rows); index++ {
			dbPrivs = append(dbPrivs, rows[index].ToString())
		}
		if isAdmin(dbPrivs) {
			privEntry.role = append(privEntry.role, []Role{READER, WRITER, ADMIN}...)
		} else {
			if isWriter(dbPrivs) {
				privEntry.role = append(privEntry.role, WRITER)
			}
			if isReader(dbPrivs) {
				privEntry.role = append(privEntry.role, READER)
			}
		}
		privMap[tableKey] = append(privMap[tableKey], privEntry)
	}
	return buildACLEntriesFromPrivMap(privMap, newACL)
}

func (tacl *tableACL) GetTablePrivFromMysqlBase(newACL func([]string) (acl.ACL, error)) (aclEntries, error) {
	ctx := context.Background()
	conn, err := tacl.conns.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	qr, err := conn.Exec(ctx, mysql.FetchTablePriv, 1000, false)
	if err != nil {
		log.Infof("loadFromMysqlBase fail %v", err)
	}
	containsAllPrivs := func(privs []string, targets []string) bool {
		for _, target := range targets {
			found := false
			for _, priv := range privs {
				if strings.EqualFold(strings.ToLower(priv), strings.ToLower(target)) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	isReader := func(privs []string) bool {
		return containsAllPrivs(privs, []string{"select"})
	}
	isWriter := func(privs []string) bool {
		return containsAllPrivs(privs, []string{"insert", "update", "delete"})
	}
	isAdmin := func(privs []string) bool {
		return containsAllPrivs(privs, []string{"select", "insert", "update", "delete", "create", "drop", "references", "index", "alter", "create view", "show view", "trigger"})
	}
	// key : 'database'.'table'
	// value : 'user'.'host', {priv}
	privMap := make(map[string][]PrivEntry)
	for _, rows := range qr.Rows {
		user := rows[0].ToString()
		host := rows[1].ToString()
		database := rows[2].ToString()
		tableName := rows[3].ToString()
		tablePrivs := strings.Split(rows[4].ToString(), ",")
		userKey := fmt.Sprintf("%s@%s", user, host)
		tableKey := fmt.Sprintf("%s.%s", database, tableName)
		privEntry := PrivEntry{
			User: userKey,
		}
		if isAdmin(tablePrivs) {
			privEntry.role = append(privEntry.role, []Role{READER, WRITER, ADMIN}...)
		} else {
			if isWriter(tablePrivs) {
				privEntry.role = append(privEntry.role, WRITER)
			}
			if isReader(tablePrivs) {
				privEntry.role = append(privEntry.role, READER)
			}
		}
		privMap[tableKey] = append(privMap[tableKey], privEntry)
	}
	return buildACLEntriesFromPrivMap(privMap, newACL)
}

// GetGlobalFromMysqlBase implement global-level authority authentication
func (tacl *tableACL) GetGlobalFromMysqlBase(newACL func([]string) (acl.ACL, error)) (aclEntries, error) {
	ctx := context.Background()
	entries := aclEntries{}
	conn, err := tacl.conns.Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	qr, err := conn.Exec(ctx, mysql.FetchGlobalPriv, 1000, false)
	if err != nil {
		log.Infof("loadFromMysqlBase fail %v", err)
	}
	var readerStrs []string
	var writerStrs []string
	var adminStrs []string
	for _, rows := range qr.Rows {
		user := rows[0].ToString()
		host := rows[1].ToString()
		selectPriv := rows[2].ToString()
		insertPriv := rows[3].ToString()
		updatePriv := rows[4].ToString()
		deletePriv := rows[5].ToString()
		superPriv := rows[6].ToString()
		userKey := BuildMysqlBasedACLKey(user, host)
		if superPriv == "Y" {
			adminStrs = append(adminStrs, userKey)
			readerStrs = append(readerStrs, userKey)
			writerStrs = append(writerStrs, userKey)
		} else {
			if selectPriv == "Y" {
				readerStrs = append(readerStrs, userKey)
			}
			if insertPriv == "Y" && updatePriv == "Y" && deletePriv == "Y" {
				writerStrs = append(writerStrs, userKey)
			}
		}
	}
	readers, err := newACL(readerStrs)
	if err != nil {
		log.Infof("readers load from readerStrs fail")
		return nil, err
	}
	writers, err := newACL(writerStrs)
	if err != nil {
		log.Infof("writers load from writerStrs fail")
		return nil, err
	}
	admins, err := newACL(adminStrs)
	if err != nil {
		log.Infof("admins load from adminStrs fail")
		return nil, err
	}
	entries = append(entries, aclEntry{
		tableNameOrPrefix: "%",
		groupName:         defaultACL,
		acl: map[Role]acl.ACL{
			READER: readers,
			WRITER: writers,
			ADMIN:  admins,
		},
	})
	sort.Sort(entries)
	return entries, nil
}

// load loads configurations from a proto-defined Config
// If err is nil, then entries is guaranteed to be non-nil (though possibly empty).
func load(config *tableaclpb.Config, newACL func([]string) (acl.ACL, error)) (entries aclEntries, err error) {
	if err := ValidateProto(config); err != nil {
		return nil, err
	}
	entries = aclEntries{}
	for _, group := range config.TableGroups {
		readers, err := newACL(group.Readers)
		if err != nil {
			return nil, err
		}
		writers, err := newACL(group.Writers)
		if err != nil {
			return nil, err
		}
		admins, err := newACL(group.Admins)
		if err != nil {
			return nil, err
		}
		for _, tableNameOrPrefix := range group.TableNamesOrPrefixes {
			entries = append(entries, aclEntry{
				tableNameOrPrefix: tableNameOrPrefix,
				groupName:         group.Name,
				acl: map[Role]acl.ACL{
					READER: readers,
					WRITER: writers,
					ADMIN:  admins,
				},
			})
		}
	}
	sort.Sort(entries)
	return entries, nil
}

func (tacl *tableACL) aclFactory() (acl.Factory, error) {
	if tacl.factory == nil {
		return GetCurrentACLFactory()
	}
	return tacl.factory, nil
}
func (tacl *tableACL) LoadFromMysql(config *tableaclpb.Config) error {
	factory, err := tacl.aclFactory()
	if err != nil {
		return err
	}
	entries, err := tacl.GetFromMysqlBase(factory.New)
	if err != nil {
		return err
	}
	tacl.Lock()
	tacl.entries = entries
	// TODO: geray can remove config?
	tacl.config = proto.Clone(config).(*tableaclpb.Config)
	callback := tacl.callback
	tacl.Unlock()
	if callback != nil {
		callback()
	}
	return nil
}
func (tacl *tableACL) Set(config *tableaclpb.Config) error {
	factory, err := tacl.aclFactory()
	if err != nil {
		return err
	}
	entries, err := load(config, factory.New)
	if err != nil {
		return err
	}
	tacl.Lock()
	tacl.entries = entries
	tacl.config = proto.Clone(config).(*tableaclpb.Config)
	callback := tacl.callback
	tacl.Unlock()
	if callback != nil {
		callback()
	}
	return nil
}

// Valid returns whether the tableACL is valid.
// Currently it only checks that it has been initialized.
func (tacl *tableACL) Valid() bool {
	tacl.RLock()
	defer tacl.RUnlock()
	return tacl.entries != nil
}

// ValidateProto returns an error if the given proto has problems
// that would cause InitFromProto to fail.
func ValidateProto(config *tableaclpb.Config) (err error) {
	t := patricia.NewTrie()
	for _, group := range config.TableGroups {
		for _, name := range group.TableNamesOrPrefixes {
			var prefix patricia.Prefix
			if strings.HasSuffix(name, "%") {
				prefix = []byte(strings.TrimSuffix(name, "%"))
			} else {
				prefix = []byte(name + "\000")
			}
			if bytes.Contains(prefix, []byte("%")) {
				return fmt.Errorf("got: %s, '%%' means this entry is a prefix and should not appear in the middle of name or prefix", name)
			}
			overlapVisitor := func(_ patricia.Prefix, item patricia.Item) error {
				return fmt.Errorf("conflicting entries: %q overlaps with %q", name, item)
			}
			if err := t.VisitSubtree(prefix, overlapVisitor); err != nil {
				return err
			}
			if err := t.VisitPrefixes(prefix, overlapVisitor); err != nil {
				return err
			}
			t.Insert(prefix, name)
		}
	}
	return nil
}

// Authorized returns the list of entities who have the specified role on a tablel.
func Authorized(table string, role Role) *ACLResult {
	return currentTableACL.Authorized(table, role)
}

// AuthorizedList returns the list of entities who have the specified role on a tablel.
func AuthorizedList(table string, role Role) []*ACLResult {
	return currentTableACL.AuthorizedList(table, role)
}

func (tacl *tableACL) AuthorizedList(table string, role Role) []*ACLResult {
	tacl.RLock()
	defer tacl.RUnlock()
	var entries []*ACLResult
	for _, entry := range tacl.entries {
		val := entry.tableNameOrPrefix
		if table == val || (strings.HasSuffix(val, "%") && strings.HasPrefix(table, val[:len(val)-1])) {
			acl, ok := entry.acl[role]
			if ok {
				entries = append(entries, &ACLResult{
					ACL:       acl,
					GroupName: entry.groupName,
				})
			}
		}
	}
	return entries
}

func (tacl *tableACL) Authorized(table string, role Role) *ACLResult {
	tacl.RLock()
	defer tacl.RUnlock()
	start := 0
	end := len(tacl.entries)
	for start < end {
		mid := start + (end-start)/2
		val := tacl.entries[mid].tableNameOrPrefix
		if table == val || (strings.HasSuffix(val, "%") && strings.HasPrefix(table, val[:len(val)-1])) {
			acl, ok := tacl.entries[mid].acl[role]
			if ok {
				return &ACLResult{
					ACL:       acl,
					GroupName: tacl.entries[mid].groupName,
				}
			}
			break
		} else if table < val {
			end = mid
		} else {
			start = mid + 1
		}
	}
	return &ACLResult{
		ACL:       acl.DenyAllACL{},
		GroupName: "",
	}
}

// GetCurrentConfig returns a copy of current tableacl configuration.
func GetCurrentConfig() *tableaclpb.Config {
	return currentTableACL.Config()
}

func (tacl *tableACL) Config() *tableaclpb.Config {
	tacl.RLock()
	defer tacl.RUnlock()
	return proto.Clone(tacl.config).(*tableaclpb.Config)
}

// Register registers an AclFactory.
func Register(name string, factory acl.Factory) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := acls[name]; ok {
		panic(fmt.Sprintf("register a registered key: %s", name))
	}
	acls[name] = factory
}

// SetDefaultACL sets the default ACL implementation.
func SetDefaultACL(name string) {
	mu.Lock()
	defer mu.Unlock()
	defaultACL = name
}

// GetCurrentACLFactory returns current table acl implementation.
func GetCurrentACLFactory() (acl.Factory, error) {
	mu.Lock()
	defer mu.Unlock()
	if len(acls) == 0 {
		return nil, fmt.Errorf("no AclFactories registered")
	}
	if defaultACL == "" {
		if len(acls) == 1 {
			for _, aclFactory := range acls {
				return aclFactory, nil
			}
		}
		return nil, errors.New("there are more than one AclFactory registered but no default has been given")
	}
	if aclFactory, ok := acls[defaultACL]; ok {
		return aclFactory, nil
	}
	return nil, fmt.Errorf("aclFactory for given default: %s is not found", defaultACL)
}
