package main

import (
	"flag"
	"fmt"
)

type CdcConfig struct {
	TableSchema         string
	SourceTableName     string
	TargetTableName     string
	TargetMetaTableName string
	FilterStatement     string
	WeScaleHost         string
	WeScaleGrpcPort     string
}

var DefaultConfig CdcConfig

func init() {
	flag.StringVar(&DefaultConfig.TableSchema, "TABLE_SCHEMA", "", "The table schema.")
	flag.StringVar(&DefaultConfig.SourceTableName, "SOURCE_TABLE_NAME", "", "The source table name.")
	flag.StringVar(&DefaultConfig.TargetTableName, "TARGET_TABLE_NAME", "", "The target table name.")
	flag.StringVar(&DefaultConfig.FilterStatement, "FILTER_STATEMENT", "", "The filter statement.")
	flag.StringVar(&DefaultConfig.WeScaleHost, "WESCALE_HOST", "127.0.0.1", "The WeScale host.")
	flag.StringVar(&DefaultConfig.WeScaleGrpcPort, "WESCALE_GRPC_PORT", "15991", "The WeScale GRPC port.")
}

func checkFlags() error {
	if DefaultConfig.TableSchema == "" {
		return fmt.Errorf("table-schema is required")
	}
	if DefaultConfig.SourceTableName == "" {
		return fmt.Errorf("table-name is required")
	}
	if DefaultConfig.FilterStatement == "" {
		return fmt.Errorf("filter-statement is required")
	}
	if DefaultConfig.WeScaleHost == "" {
		return fmt.Errorf("wescale-host is required")
	}
	if DefaultConfig.WeScaleGrpcPort == "" {
		return fmt.Errorf("wescale-grpc-port is required")
	}
	return nil
}

func mockConfig() {
	DefaultConfig.TableSchema = "d1"
	DefaultConfig.SourceTableName = "t1"
	DefaultConfig.TargetTableName = "t2"
	DefaultConfig.TargetMetaTableName = "t2_meta"
	DefaultConfig.FilterStatement = "select * from t1"
	DefaultConfig.WeScaleHost = "127.0.0.1"
	DefaultConfig.WeScaleGrpcPort = "15991"
}
