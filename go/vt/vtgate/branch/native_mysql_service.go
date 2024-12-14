package branch

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type NativeMysqlService struct {
	db *sql.DB
}

func NewMysqlService(db *sql.DB) (*NativeMysqlService, error) {
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}
	return &NativeMysqlService{db: db}, nil
}

func NewMysqlServiceWithConfig(config *mysql.Config) (*NativeMysqlService, error) {
	config.MultiStatements = true
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	service, err := NewMysqlService(db)
	if err != nil {
		db.Close()
		return nil, err
	}

	return service, nil
}

func (m *NativeMysqlService) Close() error {
	return m.db.Close()
}

func (m *NativeMysqlService) Query(query string) (Rows, error) {
	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results Rows = make([]Row, 0)

	for rows.Next() {
		// construct values container
		values := make([]interface{}, len(columns))
		for i := range values {
			var v []byte
			values[i] = &v
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		// store row values into map
		rowMap := make(map[string]Bytes)
		for i, colName := range columns {
			val := *values[i].(*[]byte)
			rowMap[colName] = val
		}

		row := Row{RowData: rowMap}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (m *NativeMysqlService) Exec(database, query string) (*Result, error) {
	ctx := context.Background()
	if database != "" {
		query = fmt.Sprintf("USE %s; %s", database, query)
	}
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	rst, err := conn.ExecContext(ctx, query)
	if err != nil {
		return nil, err
	}

	affectRows, err := rst.RowsAffected()
	if err != nil {
		return nil, err
	}
	lastInsertId, err := rst.LastInsertId()
	if err != nil {
		return nil, err
	}

	return &Result{AffectedRows: uint64(affectRows), LastInsertID: uint64(lastInsertId)}, nil
}

func (m *NativeMysqlService) ExecuteInTxn(queries ...string) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	// make sure to rollback if any query fails
	defer tx.Rollback()

	for _, query := range queries {
		_, err := tx.Exec(query)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
