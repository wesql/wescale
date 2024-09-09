/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/


package rediscdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/wesql/sqlparser/go/sqltypes"
	querypb "github.com/wesql/sqlparser/go/vt/proto/query"
	cdc "github.com/wesql/wescale-cdc"
	"google.golang.org/protobuf/encoding/prototext"
	"net"
	"os"
	"strconv"
	"strings"
)

var RedisAddr string
var RedisPWD string
var RedisDB string

func init() {
	cdc.RegisterStringVar(&RedisAddr, "REDIS_ADDRESS", "", "The redis host:port address, for example, 127.0.0.1:6379.")
	cdc.RegisterStringVar(&RedisPWD, "REDIS_PASSWORD", "", "The redis password.")
	cdc.RegisterStringVar(&RedisPWD, "REDIS_DB", "", "The redis db number.")

	cdc.SpiOpen = Open
	cdc.SpiLoadGTIDAndLastPK = LoadGTIDAndLastPK
	cdc.SpiStoreGtidAndLastPK = StoreGtidAndLastPK
	cdc.SpiStoreTableData = StoreTableData
	cdc.SpiClose = Close
	cdc.SpiInfof = func(format string, args ...any) {
		fmt.Printf(format, args...)
	}
	cdc.SpiFatalf = func(format string, args ...any) {
		fmt.Printf(format, args...)
		os.Exit(1)
	}
}

var RDBClient *redis.Client

func Open(cc *cdc.CdcConsumer) {
	err := initRedisConnect(cc)
	if err != nil {
		cdc.SpiFatalf("failed to initialize redis connection: %v", err)
	}
}

func initRedisConnect(cc *cdc.CdcConsumer) error {
	if RDBClient != nil {
		return nil
	}
	redisDB, err := strconv.Atoi(RedisDB)
	if err != nil {
		return err
	}
	opts := &redis.Options{
		Addr:     RedisAddr,
		Password: RedisPWD,
		DB:       redisDB,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return cc.DialContextFunc(ctx, addr)
		},
	}
	rdb := redis.NewClient(opts)
	RDBClient = rdb
	return nil
}

func Close(cc *cdc.CdcConsumer) {
	RDBClient.Close()
	RDBClient = nil
}

type RedisKeyType string

const (
	RedisTargetPrefix              = "wescale_cdc"
	Meta              RedisKeyType = "meta"
	TableData         RedisKeyType = "table_data"

	LastGTIDKey = "last_gtid"
	LastPKKey   = "last_pk"
)

func GenerateRedisKey(keyType RedisKeyType, pkValue string) (string, error) {
	prefix := fmt.Sprintf("%s_%s_%s", RedisTargetPrefix, cdc.DefaultConfig.TableSchema, cdc.DefaultConfig.SourceTableName)
	switch keyType {
	case Meta:
		lastPKKey := fmt.Sprintf("%s_%s", prefix, Meta)
		return lastPKKey, nil

	case TableData:
		lastPKKey := fmt.Sprintf("%s_%s:%s", prefix, TableData, pkValue)
		return lastPKKey, nil

	default:
		return "", fmt.Errorf("unknown key type: %s", keyType)
	}
}

func StoreGtidAndLastPK(currentGTID string, currentPK *querypb.QueryResult, cc *cdc.CdcConsumer) error {
	metaKey, _ := GenerateRedisKey(Meta, "")
	metaData := make(map[string]string)

	pkBytes, err := prototext.Marshal(currentPK)
	if err != nil {
		return err
	}

	metaData[LastGTIDKey] = currentGTID
	metaData[LastPKKey] = string(pkBytes)

	metaJson, err := json.Marshal(metaData)
	if err != nil {
		return err
	}

	err = RDBClient.Set(cc.Ctx, metaKey, metaJson, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func LoadGTIDAndLastPK(cc *cdc.CdcConsumer) (string, *querypb.QueryResult, error) {
	metaKey, _ := GenerateRedisKey(Meta, "")
	val, err := RDBClient.Get(cc.Ctx, metaKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil, nil
		}
		return "", nil, err
	}

	metaData := make(map[string]string)
	err = json.Unmarshal([]byte(val), &metaData)
	if err != nil {
		return "", nil, err
	}

	gtid := metaData[LastGTIDKey]

	lastPKBytes := []byte(metaData[LastPKKey])
	lastPK := querypb.QueryResult{}
	err = prototext.Unmarshal(lastPKBytes, &lastPK)
	if err != nil {
		return gtid, nil, err
	}

	return gtid, &lastPK, nil
}

func StoreTableData(resultList []*cdc.RowResult, cc *cdc.CdcConsumer) error {
	for _, rowResult := range resultList {
		switch rowResult.RowType {
		case cdc.INSERT:
			err := insertRowInRedis(rowResult.After, cc)
			if err != nil {
				return err
			}

		case cdc.UPDATE:
			// can't just set the key to the new value, because it's possible that the primary key is updated too.
			err := deleteRowInRedis(rowResult.Before, cc)
			if err != nil {
				return err
			}
			err = insertRowInRedis(rowResult.After, cc)
			if err != nil {
				return err
			}

		case cdc.DELETE:
			err := deleteRowInRedis(rowResult.Before, cc)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func generatePKValue(result *sqltypes.Result, pkFields []*querypb.Field) (string, error) {
	pkValues := make([]string, 0)
	for _, field := range pkFields {
		pkValues = append(pkValues, result.Named().Rows[0][field.Name].ToString())
	}
	if pkValues == nil {
		return "", errors.New("primary key is empty")
	}
	return strings.Join(pkValues, "-"), nil
}

func generateRowData(result *sqltypes.Result) (string, error) {
	bytes, err := json.Marshal(result.Named().Rows[0])
	return string(bytes), err
}

func insertRowInRedis(rowResult *sqltypes.Result, cc *cdc.CdcConsumer) error {
	pkValue, err := generatePKValue(rowResult, cc.PkFields)
	if err != nil {
		return err
	}
	rowData, err := generateRowData(rowResult)
	if err != nil {
		return err
	}
	rowPK, err := GenerateRedisKey(TableData, pkValue)
	if err != nil {
		return err
	}
	return RDBClient.Set(cc.Ctx, rowPK, rowData, 0).Err()
}

func deleteRowInRedis(rowResult *sqltypes.Result, cc *cdc.CdcConsumer) error {
	pkValue, err := generatePKValue(rowResult, cc.PkFields)
	if err != nil {
		return err
	}
	rowPK, err := GenerateRedisKey(TableData, pkValue)
	if err != nil {
		return err
	}
	return RDBClient.Del(cc.Ctx, rowPK).Err()
}
