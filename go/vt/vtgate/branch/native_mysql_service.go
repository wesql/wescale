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

func (m *NativeMysqlService) Query(query string) (*sql.Rows, error) {
	return m.db.Query(query)
}

func (m *NativeMysqlService) Exec(database, query string) (sql.Result, error) {
	ctx := context.Background()
	if database != "" {
		query = fmt.Sprintf("USE %s; %s", database, query)
	}
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.ExecContext(ctx, query)
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
