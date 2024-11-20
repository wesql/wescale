package branch

import (
	"fmt"
	"testing"
)

func TestGetAllDatabases(t *testing.T) {
	//todo fixme
	mysqlService, mock := NewMockMysqlService(t)
	defer mysqlService.Close()
	TargetMySQLServiceForTest := &TargetMySQLService{
		mysqlService: mysqlService,
	}

	//todo
	addMockShowCreateTable(mock, "db1", "users", "CREATE TABLE users ...")

	dbs, err := TargetMySQLServiceForTest.getAllDatabases()
	if err != nil {
		t.Error(err)
	}
	for _, db := range dbs {
		fmt.Println(db)
	}
}
