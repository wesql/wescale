/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	cdc "github.com/wesql/wescale-cdc"
	"github.com/wesql/wescale/examples/cdc/rediscdc"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	host        = "127.0.0.1"
	port        = 15306
	tableSchema = "d1"
	tableName   = "redis_test"

	RedisAddr = "127.0.0.1:6379"
	RedisPWD  = ""
	RedisDB   = "0"

	dropTableQuery      = fmt.Sprintf("drop table if exists %s", tableName)
	createTableQuery    = fmt.Sprintf("create table if not exists %s (id int primary key, name varchar(256))", tableName)
	insertInitDataQuery = fmt.Sprintf("insert into %s values (1, 'a')", tableName)
)

func mockConfig() {
	cdc.DefaultConfig.TableSchema = tableSchema
	cdc.DefaultConfig.SourceTableName = tableName
	cdc.DefaultConfig.FilterStatement = fmt.Sprintf("select * from %s", tableName)
	cdc.DefaultConfig.WeScaleHost = "127.0.0.1"
	cdc.DefaultConfig.WeScaleGrpcPort = "15991"

	rediscdc.RedisAddr = RedisAddr
	rediscdc.RedisPWD = RedisPWD
	rediscdc.RedisDB = RedisDB
}

func TestBasic(t *testing.T) {
	// init mysql
	dsn := fmt.Sprintf("(%s:%d)/%s", host, port, tableSchema)
	db, err := sql.Open("mysql", dsn)
	assert.Nil(t, err)
	defer db.Close()

	// create table
	_, err = db.Exec(createTableQuery)
	assert.Nil(t, err)

	// insert init data
	_, err = db.Exec(insertInitDataQuery)
	assert.Nil(t, err)

	// start cdc
	mockConfig()
	cc := cdc.NewCdcConsumer()
	cc.Open()
	defer cc.Close()
	go cc.Run()

	// init redis client
	redisDB, err := strconv.Atoi(RedisDB)
	assert.Nil(t, err)
	opts := &redis.Options{
		Addr:     RedisAddr,
		Password: RedisPWD,
		DB:       redisDB,
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	basicRedisTest(t, db, rdb)
}

func TestWASM(t *testing.T) {
	// init mysql
	dsn := fmt.Sprintf("(%s:%d)/%s", host, port, tableSchema)
	db, err := sql.Open("mysql", dsn)
	defer db.Close()
	assert.Nil(t, err)

	// drop table
	_, err = db.Exec(dropTableQuery)
	assert.Nil(t, err)

	// create table
	_, err = db.Exec(createTableQuery)
	assert.Nil(t, err)

	// insert init data
	_, err = db.Exec(insertInitDataQuery)
	assert.Nil(t, err)

	// run cdc in wasm
	_, err = runCMD(true, "make", "clean")
	assert.Nil(t, err)
	_, err = runCMD(true, "make", "build")
	assert.Nil(t, err)
	wasmCMD, err := runCMD(false, "make", "runwasm")
	assert.Nil(t, err)
	defer shutDownWASMProcess(t, strconv.Itoa(wasmCMD.Process.Pid))

	// init redis client
	redisDB, err := strconv.Atoi(RedisDB)
	assert.Nil(t, err)
	opts := &redis.Options{
		Addr:     RedisAddr,
		Password: RedisPWD,
		DB:       redisDB,
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	basicRedisTest(t, db, rdb)
}

func shutDownWASMProcess(t *testing.T, makePID string) {
	// there will be three processes:
	// 1.The process of make wasirun
	// 2.The subprocess of 1, which is /bin/bash -c
	// 3.The subprocess of 2, which is the actual process of wasirun
	// we should kill 3 manually, otherwise the test will not end because of process resource is not released,
	// and the rest two process will be release automatically once we kill 3.
	binBashPID := findWASIRunChildPID(makePID)
	assert.NotEqual(t, "", binBashPID)
	wasiRunPID := findWASIRunChildPID(binBashPID)
	assert.NotEqual(t, "", wasiRunPID)
	_, err := runCMD(true, "kill", "-9", wasiRunPID)
	assert.Nil(t, err)
}

func findWASIRunChildPID(makePID string) string {
	// 1.exuecte ps -ef
	cmd := exec.Command("ps", "-ef")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return ""
	}

	// filter out wasirun process
	lines := strings.Split(out.String(), "\n")
	var matchedLines []string
	for _, line := range lines {
		if strings.Contains(line, makePID) && strings.Contains(line, "wasirun") {
			matchedLines = append(matchedLines, line)
		}
	}

	if len(matchedLines) == 0 {
		return ""
	}

	// 3. use regex to extract PID and PPID
	// ps -ef output format is UID PID PPID C STIME TTY TIME CMD
	re := regexp.MustCompile(`\S+\s+(\d+)\s+(\d+)\s+\S+\s+\S+\s+\S+\s+\S+\s+.*wasirun`)
	for _, line := range matchedLines {
		matches := re.FindStringSubmatch(line)
		if len(matches) == 3 {
			pid := matches[1]  // PID
			ppid := matches[2] // PPID
			if ppid == makePID {
				return pid
			}
		}
	}
	return ""
}

func runCMD(blocked bool, c string, args ...string) (*exec.Cmd, error) {
	cmd := exec.Command(c, args...)
	cmd.Dir = "../"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return nil, err
	}
	if blocked {
		err = cmd.Wait()
	}
	return cmd, err
}

func basicRedisTest(t *testing.T, db *sql.DB, rdb *redis.Client) {
	// query init data in redis
	pkKey1, err := rediscdc.GenerateRedisKey(rediscdc.TableData, tableSchema, tableName, "1")
	assert.Nil(t, err)

	value, err := waitForData(rdb, pkKey1, true, 30*time.Second)
	assert.Nil(t, err)
	data := make(map[string]any)
	err = json.Unmarshal([]byte(value), &data)
	assert.Nil(t, err)
	assert.Equal(t, "a", data["name"])

	// insert new data in mysql, it should be found in redis
	_, err = db.Exec(fmt.Sprintf("insert into %s values (2, 'b')", tableName))
	assert.Nil(t, err)
	pkKey2, err := rediscdc.GenerateRedisKey(rediscdc.TableData, tableSchema, tableName, "2")
	assert.Nil(t, err)
	value, err = waitForData(rdb, pkKey2, true, 30*time.Second)
	assert.Nil(t, err)
	data = make(map[string]any)
	err = json.Unmarshal([]byte(value), &data)
	assert.Nil(t, err)
	assert.Equal(t, "b", data["name"])

	// update data in mysql, it should be update in redis
	_, err = db.Exec(fmt.Sprintf("update %s set id = 3, name ='c' where id = 2", tableName))
	assert.Nil(t, err)
	pkKey3, err := rediscdc.GenerateRedisKey(rediscdc.TableData, tableSchema, tableName, "3")
	assert.Nil(t, err)
	value, err = waitForData(rdb, pkKey3, true, 30*time.Second)
	assert.Nil(t, err)
	_, err = waitForData(rdb, pkKey2, false, 30*time.Second)
	assert.Nil(t, err)
	data = make(map[string]any)
	err = json.Unmarshal([]byte(value), &data)
	assert.Nil(t, err)
	assert.Equal(t, "c", data["name"])

	// delete data in mysql, it should be deleted in redis
	_, err = db.Exec(fmt.Sprintf("delete from %s ", tableName))
	assert.Nil(t, err)
	_, err = waitForData(rdb, pkKey3, false, 30*time.Second)
	assert.Nil(t, err)
	_, err = waitForData(rdb, pkKey1, false, 30*time.Second)
	assert.Nil(t, err)

	// clean meta in redis
	metaKey, err := rediscdc.GenerateRedisKey(rediscdc.Meta, tableSchema, tableName, "")
	assert.Nil(t, err)
	_, err = rdb.Del(context.Background(), metaKey).Result()
	assert.Nil(t, err)
}

func waitForData(RDBClient *redis.Client, key string, expectExist bool, timeout time.Duration) (string, error) {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		val, err := RDBClient.Get(ctx, key).Result()

		if err == redis.Nil {
			if !expectExist {
				return "", nil
			}
		} else if err != nil {
			return "", fmt.Errorf("error querying redis: %v", err)
		} else {
			// key exists
			if expectExist {
				return val, nil
			}
		}

		// retry
		time.Sleep(100 * time.Millisecond)
	}

	if expectExist {
		return "", fmt.Errorf("timeout: expected key to exist but it did not within %v", timeout)
	} else {
		return "", fmt.Errorf("timeout: expected key to not exist but it did within %v", timeout)
	}
}
