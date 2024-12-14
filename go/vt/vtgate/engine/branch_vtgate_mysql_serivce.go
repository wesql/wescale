package engine

import "vitess.io/vitess/go/vt/vtgate/branch"

type VtgateMysqlService struct {
	s       *Send
	vCursor VCursor
}

func (v *VtgateMysqlService) Query(query string) (branch.Rows, error) {
	return nil, nil
}

func (v *VtgateMysqlService) Exec(database, query string) (*branch.Result, error) {
	return nil, nil
}

func (v *VtgateMysqlService) ExecuteInTxn(queries ...string) error {
	return nil
}
