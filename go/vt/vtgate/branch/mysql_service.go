package branch

import (
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

func (m *MysqlService) ExecuteSQL(host string, port int, username, password, query string) error {
	//todo branch: "information_schema?multiStatements=true"

	// use Exec instead of Query since we're not expecting any rows to be returned
	_, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to execute SQL statements: %w", err)
	}

	return nil
}

func (m *MysqlService) ExecuteSQLInTxn(host string, port int, username, password string, queries []string) error {
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
