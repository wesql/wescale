package branch

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

// todo, think of failure handling
func (s *BranchService) BranchCreate(branchMeta *BranchMeta) error {
	// get schema from source and store to target
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

func (s *BranchService) BranchFetch(branchMeta *BranchMeta) (*BranchSchema, error) {
	schema, err := s.sourceMySQLService.FetchAndFilterCreateTableStmts(branchMeta.include, branchMeta.exclude)
	if err != nil {
		return nil, err
	}
	err = s.targetMySQLService.StoreBranchMeta(schema.schema, branchMeta) // this step is the commit point of BranchCreate function
	if err != nil {
		return nil, err
	}
	return schema, nil
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
