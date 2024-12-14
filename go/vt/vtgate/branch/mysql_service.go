package branch

import (
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)

type bytes []byte

type Row struct {
	RowData map[string]bytes
}

// Rows The order of the rows in the query result should be the same as the order in the array
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

func BytesToString(b bytes) string {
	return string(b)
}

func BytesToInt(b bytes) (int, error) {
	return strconv.Atoi(BytesToString(b))
}

func BytesToFloat64(b bytes) (float64, error) {
	return strconv.ParseFloat(BytesToString(b), 64)
}

func BytesToBool(b bytes) (bool, error) {
	return strconv.ParseBool(BytesToString(b))
}

func BytesToUint64(b bytes) (uint64, error) {
	return strconv.ParseUint(BytesToString(b), 10, 64)
}

func BytesToInt64(b bytes) (int64, error) {
	return strconv.ParseInt(BytesToString(b), 10, 64)
}
