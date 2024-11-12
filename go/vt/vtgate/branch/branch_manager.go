package branch

// todo branch

// If branch_manager.go need to call vtgate, it should call vtgate's API

type Branch struct {
}

func NewBranch() *Branch {
	return &Branch{}
}

func (b *Branch) BranchRemoteSetUrl() {
	// register branch metadata into mysql.branch table
}

func (b *Branch) BranchFetch() {
	// fetch source schema using mysql_handle.go and store it in metadata table
}

func (b *Branch) BranchCreate() {

}

func (b *Branch) BranchDiff() {
	// SchemaDiff
}

func (b *Branch) BranchCommit() {
	// PrepareMergeBack
}

func (b *Branch) BranchPush() {
	// StartMergeBack
}

func (b *Branch) BranchShow() {

}
