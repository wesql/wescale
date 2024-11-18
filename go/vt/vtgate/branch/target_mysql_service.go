package branch

type BranchMysqlHandler struct {
	host     string
	port     int
	user     string
	password string
}

func (bh *BranchMysqlHandler) ensureMetaTableExists() error {
	err := ExecuteSQL(bh.host, bh.port, bh.user, bh.password, CreateBranchMetaTableSQL)
	if err != nil {
		return err
	}
	return ExecuteSQL(bh.host, bh.port, bh.user, bh.password, CreateBranchSnapshotTableSQL)
}

func (bh *BranchMysqlHandler) getBranchFromMetaTable(workflowName string) *Branch {
	// todo
	return nil
}

func (bh *BranchMysqlHandler) executeSQLInTxn(queries []string) error {
	return ExecuteSQLInTxn(bh.host, bh.port, bh.user, bh.password, queries)
}
