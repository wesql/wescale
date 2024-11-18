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
	err := s.targetMySQLService.ensureMetaTableExists() //todo delete this
	if err != nil {
		return err
	}
	// If branch object with same name exists in BranchWorkflowCaches or branch meta table, return error
	if s.targetMySQLService.checkBranchMetaExists(branchMeta.name) {
		return fmt.Errorf("branch %v already exists", branchMeta.name)
	}

	// get schema from source
	stmts, err := s.BranchFetch(branchMeta)
	if err != nil {
		return err
	}

	// stmts act as the WAL for CreateDatabaseAndTablesIfNotExists
	err = s.targetMySQLService.CreateDatabaseAndTablesIfNotExists(stmts)
	if err != nil {
		return err
	}

	return nil
}

func (s *BranchService) BranchFetch(branchMeta BranchMeta) (map[string]map[string]string, error) {
	stmts, err := s.sourceMySQLService.FetchAndFilterCreateTableStmts(branchMeta.include, branchMeta.exclude)
	if err != nil {
		return nil, err
	}
	err = s.targetMySQLService.storeBranchMeta(stmts, branchMeta) // this step is the commit point of BranchCreate function
	if err != nil {
		return nil, err
	}
	return stmts, nil
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
