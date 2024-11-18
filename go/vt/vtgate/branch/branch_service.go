package branch

import (
	"fmt"
)

type BranchService struct {
	sourceMySQLService *SourceMySQLService
	targetMySQLService *TargetMySQLService
}

func NewBranchService(sourceHandler *SourceMySQLService, targetHandler *TargetMySQLService) *BranchService {
	return &BranchService{
		sourceMySQLService: sourceHandler,
		targetMySQLService: targetHandler,
	}
}

// todo, the func params
func (s *BranchService) BranchCreate(branchMeta BranchMeta) error {
	err := s.targetMySQLService.ensureMetaTableExists()
	if err != nil {
		return err
	}
	// If branch object with same name exists in BranchWorkflowCaches or branch meta table, return error
	if s.targetMySQLService.checkBranchExists(branchMeta.name) {
		return fmt.Errorf("branch %v already exists", branchMeta.name)
	}

	// get schema from source
	stmts, err := s.BranchFetch(branchMeta.include, branchMeta.exclude)

	// get databases from target
	databases, err := s.targetMySQLService.FetchDatabases()
	if err != nil {
		return err
	}

	// skip databases that already exist in target
	for _, db := range databases {
		delete(stmts, db)
	}

	// apply schema to target
	err = s.targetMySQLService.CreateNewDatabaseAndTables(stmts)
	if err != nil {
		return err
	}

	err = s.targetMySQLService.storeMetaData(stmts, branchMeta)
	if err != nil {
		return err
	}

	return nil
}

func (s *BranchService) BranchFetch(include, exclude string) (map[string]map[string]string, error) {
	return s.sourceMySQLService.FetchAndFilterCreateTableStmts(include, exclude)
}

// todo, get branch b from database every time?
func (b *BranchService) BranchDiff() {
	// todo
	// SchemaDiff
	// query schemas from mysql
}

func (b *BranchService) BranchPrepareMerge(meta BranchMeta) {
	// todo
	// PrepareMerge
	// get schemas from source and target through mysql connection
	// calculate diffs based on merge options such as override or merge
}

func (b *BranchService) BranchMerge() {
	// todo
	// StartMergeBack
	// apply schema diffs ddl to source through mysql connection
}

func (b *BranchService) BranchShow() {
	//todo
}
