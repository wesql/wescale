package branch

import "fmt"

// todo branch

const (
	BranchMetaTableQualifiedName = "mysql.branch"

	// the reason for using individual table for snapshot is to speed up branch meta table query
	BranchSnapshotTableQualifiedName = "mysql.branch_snapshot"
)

type BranchStatus string

type Branch struct {
	workflowName string

	// branch meta data
	// 1. source info
	sourceHost     string
	sourcePort     int
	sourceUser     string
	sourcePassword string
	// 2. target info, will not be stored in branch meta table
	targetHost     string
	targetPort     int
	targetUser     string
	targetPassword string
	// 3. filter rules
	include string
	exclude string
	// 4. others
	status string
}

// BranchWorkflowCaches map branch workflow name to branch
var BranchWorkflowCaches = make(map[string]*Branch)

func NewBranch(workflowName, sourceHost, sourcePort, sourceUser, sourcePassword, include, exclude string) *Branch {
	// todo
	return &Branch{}
}

func BranchCreate(workflowName, sourceHost, sourcePort, sourceUser, sourcePassword, include, exclude string) error {
	branchToCreate := NewBranch(workflowName, sourceHost, sourcePort, sourceUser, sourcePassword, include, exclude)

	err := branchToCreate.ensureMetaTableExists()
	if err != nil {
		return err
	}
	// If branch object with same name exists in BranchWorkflowCaches or branch meta table, return error
	if checkBranchExists(workflowName) {
		return fmt.Errorf("branch %v already exists", workflowName)
	}

	// todo
	// get schema from source, generate snapshot, snapshot equals to the source schema
	// get target schema
	// calculate schema diffs in override mode,

	// ===== txn begin =====
	// Store source Info and branch metadata into branch meta table
	// apply schema to target through mysql connection, if there is database with same name, skip
	// ===== txn commit =====
	// Create branch object in BranchWorkflowCaches

	return nil
}

func (b *Branch) BranchDiff() {
	// todo
	// SchemaDiff
	// query schemas from mysql
}

func (b *Branch) BranchPrepareMerge() {
	// todo
	// PrepareMerge
	// get schemas from source and target through mysql connection
	// calculate diffs based on merge options such as override or merge
}

func (b *Branch) BranchMerge() {
	// todo
	// StartMergeBack
	// apply schema diffs ddl to source through mysql connection
}

func (b *Branch) BranchShow() {
	//todo
}

// #####################################################################
// from now onwards are helper functions

// Ensure branch meta table exists in target mysql, if not exists, create it
func (b *Branch) ensureMetaTableExists() error {
	// todo
	return nil
}

func checkBranchExists(workflowName string) bool {
	// check workflowName exists in BranchWorkflowCaches
	if _, exists := BranchWorkflowCaches[workflowName]; exists {
		return true
	}

	// check from getBranchFromMetaTable  branch meta table
	if getBranchFromMetaTable(workflowName) != nil {
		return true
	}
	return false
}

func getBranchFromMetaTable(workflowName string) *Branch {
	// todo
	return nil
}

func fetchSchema(host, port, user, password, include, exclude string) []string {
	// fetch source schema
	// the schemas are filtered by `include` and `exclude` rules
	// todo

	return nil
}

func applySchema(host, port, user, password, DDLs []string) error {
	return nil
}
