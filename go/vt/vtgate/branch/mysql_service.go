package branch

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlService struct {
	db *sql.DB
}

func NewMysqlService(db *sql.DB) (*MysqlService, error) {
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}
	return &MysqlService{db: db}, nil
}

func NewMysqlServiceWithConfig(config *mysql.Config) (*MysqlService, error) {
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

func (m *MysqlService) Close() error {
	return m.db.Close()
}

func (m *MysqlService) Query(query string) (*sql.Rows, error) {
	return m.db.Query(query)
}

func (m *MysqlService) Exec(database, query string) (sql.Result, error) {
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

func (m *MysqlService) ExecuteInTxn(queries ...string) error {
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
