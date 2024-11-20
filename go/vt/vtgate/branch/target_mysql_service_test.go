package branch

import (
	"testing"
)

func TestGetAllDatabases(t *testing.T) {
	mysqlService, mock := NewMockMysqlService(t)
	defer mysqlService.Close()
	TargetMySQLServiceForTest := &TargetMySQLService{
		mysqlService: mysqlService,
	}

	InitMockShowDatabases(mock)

	_, err := TargetMySQLServiceForTest.getAllDatabases()
	if err != nil {
		t.Error(err)
	}
}
