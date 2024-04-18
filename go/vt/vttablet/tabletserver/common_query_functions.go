/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package tabletserver

import (
	"context"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
)

// CommonQuery is a universal RPC interface.
// If you need an RPC interface to retrieve query results from a tablet and the return type is *sqltypes.Result,
// you can directly use this RPC interface without having to create a new one.
// The specific process is as follows: Assuming your RPC request handling function is named f, with parameters x and y.
// 1. Call the CommonQuery RPC on the vtgate side, set the queryFunctionName parameter to the function name f,
// and set the queryFunctionArgs parameter to a map constructed with x and y as keys and the specific calling values as values.
// 2. In the CommonQuery below, add codes: when the queryFunctionName is f, use the parameters stored in queryFunctionArgs to call f.
func (tsv *TabletServer) CommonQuery(_ context.Context, queryFunctionName string, queryFunctionArgs map[string]any) (*sqltypes.Result, error) {
	// Distribute requests to specific functions based on their names
	switch queryFunctionName {
	case "TabletsPlans":
		return tsv.qe.TabletsPlans(tsv.alias)
	case "HandleWescaleFilterRequest":
		return tsv.qe.HandleWescaleFilterRequest(queryFunctionArgs["sql"].(string))
	default:
		return nil, fmt.Errorf("query function %s not found", queryFunctionName)
	}
}

func BuildVarCharFields(names ...string) []*querypb.Field {
	fields := make([]*querypb.Field, len(names))
	for i, v := range names {
		fields[i] = &querypb.Field{
			Name:    v,
			Type:    sqltypes.VarChar,
			Charset: collations.CollationUtf8ID,
			Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
		}
	}
	return fields
}

func BuildVarCharRow(values ...string) []sqltypes.Value {
	row := make([]sqltypes.Value, len(values))
	for i, v := range values {
		row[i] = sqltypes.NewVarChar(v)
	}
	return row
}

// todo remove origin one
func (qe *QueryEngine) TabletsPlans(alias *topodatapb.TabletAlias) (*sqltypes.Result, error) {
	rows := [][]sqltypes.Value{}

	formattedAlias := fmt.Sprintf("%v-%v", alias.Cell, alias.Uid)

	qe.plans.ForEach(func(value any) bool {
		plan := value.(*TabletPlan)

		tablesStr := ""
		isFirst := true
		for _, p := range plan.Plan.Permissions {
			if !isFirst {
				tablesStr += ","
			}
			isFirst = false
			tablesStr += fmt.Sprintf("%v.%v", p.Database, p.TableName)
		}

		var pqstats perQueryStats
		pqstats.QueryCount, pqstats.Time, pqstats.MysqlTime, pqstats.RowsAffected, pqstats.RowsReturned, pqstats.ErrorCount = plan.Stats()

		rows = append(rows, BuildVarCharRow(
			formattedAlias,
			sqlparser.TruncateForUI(plan.Original),
			plan.PlanID.String(),
			tablesStr,
			strconv.FormatUint(pqstats.QueryCount, 10),
			pqstats.Time.String(),
			pqstats.MysqlTime.String(),
			strconv.FormatUint(pqstats.RowsAffected, 10),
			strconv.FormatUint(pqstats.RowsReturned, 10),
			strconv.FormatUint(pqstats.ErrorCount, 10),
		))
		return true
	})

	return &sqltypes.Result{
		Fields: BuildVarCharFields("tablet_alias", "query_template", "plan_type", "tables", "query_count", "accumulated_time", "accumulated_mysql_time", "rows_affected", "rows_returned", "error_count"),
		Rows:   rows,
	}, nil
}

func (qe *QueryEngine) HandleWescaleFilterRequest(sql string) (*sqltypes.Result, error) {

	stmt, _, err := sqlparser.Parse2(sql)
	if err != nil {
		return nil, err
	}

	switch s := stmt.(type) {
	case *sqlparser.CreateWescaleFilter:
		return qe.HandleCreateFilter(s)
	case *sqlparser.AlterWescaleFilter:
		return qe.HandleAlterFilter(s)
	}

	return nil, fmt.Errorf("stmt type is not support: %v", stmt)
}
