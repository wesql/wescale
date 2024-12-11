package branch

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlService interface {
	Query(query string) (*sql.Rows, error)
	Exec(database, query string) (sql.Result, error)
	ExecuteInTxn(queries ...string) error
}
