package engine

import (
	"context"
	"fmt"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/branch"
)

type VtgateMysqlService struct {
	vCursor VCursor
}

func (v *VtgateMysqlService) Query(query string) (branch.Rows, error) {
	// AUTOCOMMIT is used to run the statement as autocommitted transaction.
	// AUTOCOMMIT = 3;
	rst, err := v.vCursor.Execute(context.Background(), "Execute", query, make(map[string]*querypb.BindVariable), true, 3)
	if err != nil {
		return nil, err
	}

	var results branch.Rows = make([]branch.Row, 0)

	for _, r := range rst.Rows {
		if len(rst.Fields) != len(r) {
			return nil, fmt.Errorf("field length not equal to row value length")
		}
		rowMap := make(map[string]branch.Bytes)
		for i, v := range r {
			bytes, err := v.ToBytes()
			if err != nil {
				return nil, err
			}
			rowMap[rst.Fields[i].Name] = bytes
		}
		results = append(results, branch.Row{RowData: rowMap})
	}

	return results, nil
}

func (v *VtgateMysqlService) Exec(database, query string) (*branch.Result, error) {
	_, err := v.vCursor.Execute(context.Background(), "Execute", fmt.Sprintf("USE `%s`", database), make(map[string]*querypb.BindVariable), true, 3)
	if err != nil {
		return nil, err
	}
	rst, err := v.vCursor.Execute(context.Background(), "Execute", query, make(map[string]*querypb.BindVariable), true, 3)
	if err != nil {
		return nil, err
	}

	return &branch.Result{AffectedRows: rst.RowsAffected, LastInsertID: rst.InsertID}, nil
}

func (v *VtgateMysqlService) ExecuteInTxn(queries ...string) error {
	first := true
	defer v.vCursor.Execute(context.Background(), "Execute", "ROLLBACK;", make(map[string]*querypb.BindVariable), true, 0)
	for _, query := range queries {
		if first {
			_, err := v.vCursor.Execute(context.Background(), "Execute", "start transaction;", make(map[string]*querypb.BindVariable), true, 0)
			if err != nil {
				return err
			}
			first = false
		}
		// NORMAL is the default commit order.
		// NORMAL = 0;
		_, err := v.vCursor.Execute(context.Background(), "Execute", query, make(map[string]*querypb.BindVariable), true, 0)
		if err != nil {
			return err
		}
	}

	_, err := v.vCursor.Execute(context.Background(), "Execute", "COMMIT;", make(map[string]*querypb.BindVariable), true, 0)
	return err
}
