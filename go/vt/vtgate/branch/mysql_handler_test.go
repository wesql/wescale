package branch

import (
	"testing"
)

var (
	workflowName   = "mysql branch test"
	sourceHost     = "127.0.0.1"
	sourcePort     = 17100
	sourceUser     = ""
	sourcePassword = ""
	targetHost     = "127.0.0.1"
	targetPort     = 17101
	targetUser     = ""
	targetPassword = ""
	include        = "branch_source.*"
	exclude        = "*.t1"

	sourceHandler = &BranchMysqlHandler{host: sourceHost, port: sourcePort, user: sourceUser, password: sourcePassword}
	targetHandler = &BranchMysqlHandler{host: targetHost, port: targetPort, user: targetUser, password: targetPassword}
)

func TestBranchCreate(t *testing.T) {
	err := BranchCreate(workflowName, sourceHost, sourcePort, sourceUser, sourcePassword, targetHost, targetPort, targetUser, targetPassword, include, exclude, sourceHandler, targetHandler)
	if err != nil {
		t.Fatal(err)
	}
}
