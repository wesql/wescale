package branch

import (
	_ "github.com/go-sql-driver/mysql"
)

type Row struct {
	RowData map[string][]byte
}

type Rows []Row

type Result struct {
	LastInsertID int64
	AffectedRows int64
}

type MysqlService interface {
	Query(query string) (Rows, error)
	Exec(database, query string) (*Result, error)
	ExecuteInTxn(queries ...string) error
}
