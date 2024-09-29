/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package wrangler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/schema"

	"google.golang.org/protobuf/proto"

	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"

	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sidecardb"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/mysqlctl/tmutils"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vtctl/schematools"

	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/sqlparser"
)

type BranchJob struct {
	wr               *Wrangler
	bs               *vtctldatapb.BranchSettings
	sourceDatabase   string
	targetDatabase   string
	workflowName     string
	sourceTopo       string
	sourceTabletType string
	externalCluster  string
	includeTables    string
	excludeTables    string
	cells            string
	skipCopyPhase    bool
	stopAfterCopy    bool
	onddl            string
	status           string
	mergeTimestamp   string
}

const (
	BranchStatusOfPrePare = "Prepare"
	BranchStatusOfRunning = "Running"
)

const (
	OutputTypeCreateTable = "create_table"
	OutputTypeDDL         = "ddl"
	OutputTypeConflict    = "conflict"

	CompareObjectsSourceTarget   = "source_target"
	CompareObjectsTargetSource   = "target_source"
	CompareObjectsSourceSnapshot = "source_snapshot"
	CompareObjectsSnapshotSource = "snapshot_source"
	CompareObjectsTargetSnapshot = "target_snapshot"
	CompareObjectsSnapshotTarget = "snapshot_target"

	MergeOptionOverride = "override"
	MergeOptionDiff     = "diff"
)

const InsertTableRulesTemplate = "INSERT INTO mysql.branch_table_rules " +
	"(workflow_name, source_table_name, target_table_name, filtering_rule, create_ddl, merge_ddl, need_merge_back) " +
	"VALUES (%a, %a, %a, %a, %a, %a,1);"

const UpdateMergeDDLTemplate = "UPDATE mysql.branch_table_rules set merge_ddl=%a, need_merge_back=1 where workflow_name=%a and target_table_name=%a;"

const UpdateWorkflowNeedMergeBackFalse = "UPDATE mysql.branch_table_rules set need_merge_back=0 where workflow_name=%a;"

const UpdateWorkflowMergeTimestamp = "UPDATE mysql.branch_jobs set merge_timestamp=%a where workflow_name=%a;"

const SelectBranchJobByWorkflow = "select * from mysql.branch_jobs where workflow_name = '%s'"

const SelectBranchTableRuleByWorkflow = "select * from mysql.branch_table_rules where workflow_name = '%s'"

const SelectBranchTableRuleByWorkflowAndTableName = "select * from mysql.branch_table_rules where workflow_name=%a and source_table_name=%a and target_table_name=%a"

const UpdateMergeDDLByWorkFlowAndTableName = "update mysql.branch_table_rules set merge_ddl=%a, need_merge_back=1 where workflow_name=%a and source_table_name=%a and target_table_name=%a"

const DropTableAndDropViewTemplate = "DROP TABLE IF EXISTS `%s`"

const DeleteBranchJobByWorkflow = "DELETE FROM mysql.branch_jobs where workflow_name='%s'"

const DeleteBranchSnapshotByWorkflow = "DELETE FROM mysql.branch_snapshots where workflow_name='%s'"

const DeleteBranchTableRuleByWorkflow = "DELETE FROM mysql.branch_table_rules where workflow_name='%s'"

const DeleteVReplicationByWorkFlow = "DELETE FROM mysql.vreplication where workflow='%s'"

func (branchJob *BranchJob) generateInsert() (string, error) {
	// build the query
	sqlInsertTemplate := "INSERT INTO mysql.branch_jobs (source_database, target_database, workflow_name, source_topo, source_tablet_type, cells, stop_after_copy, onddl, status, message,external_cluster) VALUES (%a, %a, %a, %a, %a, %a, %a, %a, %a, %a, %a)"

	// bind variables
	sqlInsertQuery, err := sqlparser.ParseAndBind(sqlInsertTemplate,
		sqltypes.StringBindVariable(branchJob.sourceDatabase),
		sqltypes.StringBindVariable(branchJob.targetDatabase),
		sqltypes.StringBindVariable(branchJob.workflowName),
		sqltypes.StringBindVariable(branchJob.sourceTopo),
		sqltypes.StringBindVariable(branchJob.sourceTabletType),
		sqltypes.StringBindVariable(branchJob.cells),
		sqltypes.Int64BindVariable(boolToInt(branchJob.stopAfterCopy)),
		sqltypes.StringBindVariable(branchJob.onddl),
		sqltypes.StringBindVariable(branchJob.status),
		sqltypes.StringBindVariable(""), // empty message
		sqltypes.StringBindVariable(branchJob.externalCluster),
	)
	if err != nil {
		return "", err
	}

	return sqlInsertQuery, nil
}

// Helper function to convert bool to int (0 or 1) for tinyint fields
func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func (branchJob *BranchJob) generateRulesInsert() (string, error) {
	sqlInsertTemplate := "INSERT INTO mysql.branch_table_rules (workflow_name, source_table_name, target_table_name, filtering_rule, create_ddl, merge_ddl, default_filter_rules,skip_copy_phase) VALUES "

	valuesPlaceholders := []string{}
	var bindVariables []*querypb.BindVariable

	for _, tableRule := range branchJob.bs.FilterTableRules {
		valuesPlaceholders = append(valuesPlaceholders, "(%a, %a, %a, %a, %a, %a, %a, %a)")
		bindVariables = append(bindVariables,
			sqltypes.StringBindVariable(branchJob.workflowName),
			sqltypes.StringBindVariable(tableRule.SourceTable),
			sqltypes.StringBindVariable(tableRule.TargetTable),
			sqltypes.StringBindVariable(tableRule.FilteringRule),
			sqltypes.StringBindVariable(tableRule.CreateDdl),
			sqltypes.StringBindVariable(tableRule.MergeDdl),
			sqltypes.StringBindVariable(tableRule.DefaultFilterRules),
			sqltypes.Int64BindVariable(boolToInt(tableRule.SkipCopyPhase)),
		)
	}

	fullQuery := sqlInsertTemplate + strings.Join(valuesPlaceholders, ",")

	sqlQuery, err := sqlparser.ParseAndBind(fullQuery, bindVariables...)
	if err != nil {
		return "", err
	}

	return sqlQuery + ";", nil
}

// PrepareBranch should insert BranchSettings data into mysql.branch_setting
func (wr *Wrangler) PrepareBranch(ctx context.Context, workflow, sourceDatabase, targetDatabase,
	cell, tabletTypes string, includeTables, excludeTables string, stopAfterCopy bool, defaultFilterRules string, skipCopyPhase bool, externalCluster string) error {
	// 1.get source vschema
	var externalTopo *topo.Server
	var vschema *vschemapb.Keyspace
	var tables []string
	var err error
	if externalCluster != "" {
		// when the source is an external mysql cluster mounted using the Mount command
		externalTopo, err = wr.ts.OpenExternalVitessClusterServer(ctx, externalCluster)
		if err != nil {
			return err
		}
		wr.sourceTs = externalTopo
		log.Infof("Successfully opened external topo: %+v", externalTopo)
	}
	if externalCluster != "" {
		vschema, err = wr.sourceTs.GetVSchema(ctx, sourceDatabase)
	} else {
		vschema, err = wr.ts.GetVSchema(ctx, sourceDatabase)
	}
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return fmt.Errorf("source database %s not found", sourceDatabase)
		}
		return err
	}
	if vschema == nil {
		return fmt.Errorf("no vschema found for target keyspace %s", targetDatabase)
	}

	// 2.get source keyspace tables
	if strings.HasPrefix(includeTables, "{") {
		if vschema.Tables == nil {
			vschema.Tables = make(map[string]*vschemapb.Table)
		}
		wrap := fmt.Sprintf(`{"tables": %s}`, includeTables)
		ks := &vschemapb.Keyspace{}
		if err := json2.Unmarshal([]byte(wrap), ks); err != nil {
			return err
		}
		for table, vtab := range ks.Tables {
			vschema.Tables[table] = vtab
			tables = append(tables, table)
		}
	} else {
		if len(strings.TrimSpace(includeTables)) > 0 {
			tables = strings.Split(includeTables, ",")
		}
		ksTables, err := wr.getKeyspaceTables(ctx, sourceDatabase, wr.sourceTs)
		if err != nil {
			return err
		}
		if len(tables) > 0 {
			err = wr.validateSourceTablesExist(ctx, sourceDatabase, ksTables, tables)
			if err != nil {
				return err
			}
		} else {
			tables = ksTables
		}
		var excludeTablesList []string
		excludeTables = strings.TrimSpace(excludeTables)
		if excludeTables != "" {
			excludeTablesList = strings.Split(excludeTables, ",")
			err = wr.validateSourceTablesExist(ctx, sourceDatabase, ksTables, excludeTablesList)
			if err != nil {
				return err
			}
		}
		var tables2 []string
		for _, t := range tables {
			if shouldInclude(t, excludeTablesList) {
				tables2 = append(tables2, t)
			}
		}
		tables = tables2
		if len(tables) == 0 {
			return fmt.Errorf("no tables to move")
		}
		log.Infof("Found tables to move: %s", strings.Join(tables, ","))
	}
	createDDLMode := createDDLAsCopy

	// create target database
	err = wr.CreateDatabase(ctx, targetDatabase)

	// create branch job
	if err != nil {
		return err
	}
	branchJob := &BranchJob{
		status:           BranchStatusOfPrePare,
		workflowName:     workflow,
		sourceDatabase:   sourceDatabase,
		targetDatabase:   targetDatabase,
		sourceTabletType: tabletTypes,
		externalCluster:  externalCluster,
		includeTables:    includeTables,
		excludeTables:    excludeTables,
		stopAfterCopy:    stopAfterCopy,
		cells:            cell,
	}
	branchJob.status = BranchStatusOfPrePare
	branchJob.bs = &vtctldatapb.BranchSettings{}
	insert, err := branchJob.generateInsert()
	if err != nil {
		return err
	}
	alias, err := wr.GetPrimaryTabletAlias(ctx, branchJob.cells)
	if err != nil {
		return err
	}

	// generate filterTableRule
	for _, table := range tables {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v ", sqlparser.NewIdentifierCS(table))
		if defaultFilterRules != "" {
			buf.WriteString(fmt.Sprintf("WHERE %v", defaultFilterRules))
		}
		filterTableRule := &vtctldatapb.FilterTableRule{
			SourceTable:        table,
			TargetTable:        table,
			FilteringRule:      buf.String(),
			CreateDdl:          createDDLMode,
			MergeDdl:           createDDLMode,
			DefaultFilterRules: defaultFilterRules,
			SkipCopyPhase:      skipCopyPhase,
		}
		branchJob.bs.FilterTableRules = append(branchJob.bs.FilterTableRules, filterTableRule)
	}
	// get insert filterTableRule sql
	rulesInsert, err := branchJob.generateRulesInsert()
	if err != nil {
		return err
	}
	_, err = wr.ExecuteFetchAsDba(ctx, alias, "START TRANSACTION", 1, false, false)
	if err != nil {
		return err
	}
	_, err = wr.ExecuteFetchAsDba(ctx, alias, insert, 1, false, false)
	if err != nil {
		return err
	}
	_, err = wr.ExecuteFetchAsDba(ctx, alias, rulesInsert, 1, false, false)
	if err != nil {
		return err
	}
	_, err = wr.ExecuteFetchAsDba(ctx, alias, "COMMIT", 1, false, false)
	if err != nil {
		return err
	}
	wr.Logger().Printf("successfully create branch workflow : %v sourceDatabase : %v targetDatabase : %v\n", branchJob.workflowName, branchJob.sourceDatabase, branchJob.targetDatabase)
	wr.Logger().Printf("rules : \n")
	for _, rule := range branchJob.bs.FilterTableRules {
		wr.Logger().Printf("[%v]\n", rule)
	}
	return nil
}

func GetBranchJobByWorkflow(ctx context.Context, workflow string, wr *Wrangler) (*BranchJob, error) {
	sql := fmt.Sprintf(SelectBranchJobByWorkflow, workflow)
	result, err := wr.ExecuteQueryByPrimary(ctx, sql, false, false)
	qr := sqltypes.Proto3ToResult(result)
	if err != nil {
		return nil, err
	}
	if qr == nil || len(qr.Rows) == 0 {
		return nil, fmt.Errorf("workflow:%v not exist", workflow)
	}
	branchJobMap := qr.Named().Row()
	sourceDatabase := branchJobMap["source_database"].ToString()
	targetDatabase := branchJobMap["target_database"].ToString()
	sourceTopo := branchJobMap["source_topo"].ToString()
	sourceTabletType := branchJobMap["source_tablet_type"].ToString()
	cells := branchJobMap["cells"].ToString()
	stopAfterCopy, err := branchJobMap["stop_after_copy"].ToBool()
	externalCluster := branchJobMap["external_cluster"].ToString()
	if err != nil {
		return nil, err
	}
	onddl := branchJobMap["onddl"].ToString()
	status := branchJobMap["status"].ToString()
	mergeTimestamp := branchJobMap["merge_timestamp"].ToString()
	branchJob := &BranchJob{
		sourceDatabase:   sourceDatabase,
		targetDatabase:   targetDatabase,
		workflowName:     workflow,
		sourceTopo:       sourceTopo,
		sourceTabletType: sourceTabletType,
		externalCluster:  externalCluster,
		stopAfterCopy:    stopAfterCopy,
		onddl:            onddl,
		cells:            cells,
		status:           status,
		mergeTimestamp:   mergeTimestamp,
	}
	branchJob.bs, err = GetBranchTableRulesByWorkflow(ctx, workflow, wr)
	if err != nil {
		return nil, err
	}
	return branchJob, nil
}

func GetBranchTableRulesByWorkflow(ctx context.Context, workflow string, wr *Wrangler) (*vtctldatapb.BranchSettings, error) {
	bs := &vtctldatapb.BranchSettings{}
	alias, err := wr.GetPrimaryTabletAlias(ctx, sidecardb.DefaultCellName)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(SelectBranchTableRuleByWorkflow, workflow)
	result, err := wr.ExecuteFetchAsDba(ctx, alias, sql, 1000, false, false)
	qr := sqltypes.Proto3ToResult(result)
	if err != nil {
		return nil, err
	}
	for _, tableRules := range qr.Named().Rows {
		id, err := tableRules["id"].ToUint64()
		if err != nil {
			return nil, err
		}
		sourceTableName := tableRules["source_table_name"].ToString()
		targetTableName := tableRules["target_table_name"].ToString()
		filterRule := tableRules["filtering_rule"].ToString()
		createDDL := tableRules["create_ddl"].ToString()
		mergeDDL := tableRules["merge_ddl"].ToString()
		mergeUUID := tableRules["merge_ddl_uuid"].ToString()
		needMergeBack, err := tableRules["need_merge_back"].ToBool()
		if err != nil {
			return nil, err
		}
		skipCopyPhase, err := tableRules["skip_copy_phase"].ToBool()
		if err != nil {
			return nil, err
		}
		fileterRule := &vtctldatapb.FilterTableRule{
			Id:            id,
			SourceTable:   sourceTableName,
			TargetTable:   targetTableName,
			FilteringRule: filterRule,
			CreateDdl:     createDDL,
			MergeDdl:      mergeDDL,
			NeedMergeBack: needMergeBack,
			MergeDdlUuid:  mergeUUID,
			SkipCopyPhase: skipCopyPhase,
		}
		bs.FilterTableRules = append(bs.FilterTableRules, fileterRule)
	}
	return bs, nil

}

func removeComments(sql string) (string, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}
	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		switch cursor.Node().(type) {
		case *sqlparser.ParsedComments:
			cursor.Replace(nil)
		}
		return true
	}, nil)

	return sqlparser.String(stmt), nil
}

func (wr *Wrangler) ExecuteQueryByPrimary(ctx context.Context, sql string, disableBinlog, reloadSchema bool) (*querypb.QueryResult, error) {
	//todo earayu: should not use sidecardb.DefaultCellName here
	alias, err := wr.GetPrimaryTabletAlias(ctx, sidecardb.DefaultCellName)
	if err != nil {
		return nil, err
	}
	return wr.ExecuteFetchAsDba(ctx, alias, sql, 10000, disableBinlog, reloadSchema)
}

func (wr *Wrangler) RebuildMaterializeSettings(ctx context.Context, workflow string) (*vtctldatapb.MaterializeSettings, error) {
	branchJob, err := GetBranchJobByWorkflow(ctx, workflow, wr)
	if err != nil {
		return nil, err
	}
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:              workflow,
		MaterializationIntent: vtctldatapb.MaterializationIntent_BRANCH,
		SourceKeyspace:        branchJob.sourceDatabase,
		TargetKeyspace:        branchJob.targetDatabase,
		Cell:                  branchJob.cells,
		TabletTypes:           branchJob.sourceTabletType,
		StopAfterCopy:         branchJob.stopAfterCopy,
		ExternalCluster:       branchJob.externalCluster,
	}
	if ms.ExternalCluster != "" {
		// when the source is an external mysql cluster mounted using the Mount command
		externalTopo, err := wr.ts.OpenExternalVitessClusterServer(ctx, ms.ExternalCluster)
		if err != nil {
			return nil, err
		}
		wr.sourceTs = externalTopo
		log.Infof("Successfully opened external topo: %+v", externalTopo)
	}
	for _, rule := range branchJob.bs.FilterTableRules {
		removedSQL, err := removeComments(rule.FilteringRule)
		if err != nil {
			return nil, err
		}
		rule.FilteringRule = removedSQL
		ts := &vtctldatapb.TableMaterializeSettings{
			TargetTable:      rule.TargetTable,
			SourceExpression: rule.FilteringRule,
			CreateDdl:        rule.CreateDdl,
			SkipCopyPhase:    rule.SkipCopyPhase,
		}
		ms.TableSettings = append(ms.TableSettings, ts)
	}
	return ms, nil
}

func (wr *Wrangler) StreamExist(ctx context.Context, workflow string) (bool, error) {
	sql := fmt.Sprintf("SELECT 1 FROM mysql.vreplication WHERE workflow='%s';", workflow)
	result, err := wr.ExecuteQueryByPrimary(ctx, sql, false, false)
	if err != nil {
		return false, err
	}
	if len(result.Rows) != 0 {
		return true, nil
	}
	return false, nil
}

func (wr *Wrangler) StartBranch(ctx context.Context, workflow string) error {
	ms, err := wr.RebuildMaterializeSettings(ctx, workflow)
	if err != nil {
		return err
	}
	var exist bool
	var mz *materializer
	if exist, err = wr.StreamExist(ctx, workflow); err != nil {
		return err
	}
	if !exist {
		mz, err = wr.prepareMaterializerStreams(ctx, ms)
		if err != nil {
			return err
		}
		err = wr.storeSchemaSnapshot(ctx, workflow, mz)
		if err != nil {
			return err
		}
	} else {
		mz, err = wr.buildMaterializer(ctx, ms)
		if err != nil {
			return err
		}
	}
	exist, err = wr.StreamExist(ctx, workflow)
	if err != nil {
		return err
	}
	if exist {
		err = mz.startStreams(ctx)
		if err != nil {
			return err
		}
	}
	wr.Logger().Printf("Start workflow:%v successfully.", workflow)
	return nil
}

func (wr *Wrangler) StopBranch(ctx context.Context, workflow string) error {
	ms, err := wr.RebuildMaterializeSettings(ctx, workflow)
	if err != nil {
		return err
	}
	mz, err := wr.buildMaterializer(ctx, ms)
	if err != nil {
		return err
	}
	exist, err := wr.StreamExist(ctx, workflow)
	if err != nil {
		return err
	}
	if exist {
		err = mz.stopStreams(ctx)
		if err != nil {
			return err
		}
	}
	wr.Logger().Printf("Start workflow %v successfully", workflow)
	return nil
}
func analyzeDiffSchema(source *tabletmanagerdatapb.SchemaDefinition, target *tabletmanagerdatapb.SchemaDefinition) ([]schemadiff.EntityDiff, error) {
	sourceSchema, err := transformSchemaDefinitionToSchema(source)
	if err != nil {
		return nil, err
	}
	targetSchema, err := transformSchemaDefinitionToSchema(target)
	if err != nil {
		return nil, err
	}
	return getSchemaDiff(sourceSchema, targetSchema)
}

func getSchemaDiff(sourceSchema *schemadiff.Schema, targetSchema *schemadiff.Schema) ([]schemadiff.EntityDiff, error) {
	hint := schemadiff.DiffHints{}
	diffEntries, err := sourceSchema.Diff(targetSchema, &hint)
	if err != nil {
		return nil, err
	}
	newTargetSchema, err := sourceSchema.Apply(diffEntries)
	if err != nil {
		return nil, err
	}
	if targetSchema.ToSQL() != newTargetSchema.ToSQL() {
		return nil, fmt.Errorf("analyze diff error")
	}
	return diffEntries, nil
}

func (wr *Wrangler) GenerateUpdateOrInsertNewTable(ctx context.Context, diffEntry schemadiff.EntityDiff, branchJob *BranchJob) (string, error) {
	switch entry := diffEntry.Statement().(type) {
	case *sqlparser.CreateTable, *sqlparser.CreateView:
		// add element into
		var tableName string
		if createTableStmt, ok := entry.(*sqlparser.CreateTable); ok {
			tableName = createTableStmt.Table.Name.String()
		} else if createTableStmt, ok := entry.(*sqlparser.CreateView); ok {
			tableName = createTableStmt.ViewName.Name.String()
		}
		querySQL, err := sqlparser.ParseAndBind(SelectBranchTableRuleByWorkflowAndTableName,
			sqltypes.StringBindVariable(branchJob.workflowName),
			sqltypes.StringBindVariable(tableName),
			sqltypes.StringBindVariable(tableName))
		if err != nil {
			return "", err
		}
		qr, err := wr.ExecuteQueryByPrimary(ctx, querySQL, false, false)
		if err != nil {
			return "", err
		}
		var sqlInsertQuery string
		if len(qr.Rows) != 0 {
			sqlInsertQuery, err = sqlparser.ParseAndBind(UpdateMergeDDLByWorkFlowAndTableName,
				sqltypes.StringBindVariable(diffEntry.CanonicalStatementString()),
				sqltypes.StringBindVariable(branchJob.workflowName),
				sqltypes.StringBindVariable(tableName),
				sqltypes.StringBindVariable(tableName),
			)
			if err != nil {
				return "", err
			}
		} else {
			sqlInsertQuery, err = sqlparser.ParseAndBind(InsertTableRulesTemplate,
				sqltypes.StringBindVariable(branchJob.workflowName),
				sqltypes.StringBindVariable(tableName),
				sqltypes.StringBindVariable(tableName),
				sqltypes.StringBindVariable("Merge back new table is not needed."),
				sqltypes.StringBindVariable("Merge back new table is not needed."),
				sqltypes.StringBindVariable(diffEntry.CanonicalStatementString()),
			)
			if err != nil {
				return "", err
			}
		}
		return sqlInsertQuery, nil
	case *sqlparser.DropTable:
		sqlBuf := strings.Builder{}
		for _, table := range entry.FromTables {
			tableName := table.Name.String()
			querySQL, err := sqlparser.ParseAndBind(SelectBranchTableRuleByWorkflowAndTableName,
				sqltypes.StringBindVariable(branchJob.workflowName),
				sqltypes.StringBindVariable(tableName),
				sqltypes.StringBindVariable(tableName))
			if err != nil {
				return "", err
			}
			qr, err := wr.ExecuteQueryByPrimary(ctx, querySQL, false, false)
			if err != nil {
				return "", err
			}
			var sqlInsertQuery string
			dropQuery := fmt.Sprintf(DropTableAndDropViewTemplate, tableName)
			if len(qr.Rows) != 0 {
				sqlInsertQuery, err = sqlparser.ParseAndBind(UpdateMergeDDLByWorkFlowAndTableName,
					sqltypes.StringBindVariable(dropQuery),
					sqltypes.StringBindVariable(branchJob.workflowName),
					sqltypes.StringBindVariable(tableName),
					sqltypes.StringBindVariable(tableName),
				)
				if err != nil {
					return "", err
				}
			} else {
				sqlInsertQuery, err = sqlparser.ParseAndBind(InsertTableRulesTemplate,
					sqltypes.StringBindVariable(branchJob.workflowName),
					sqltypes.StringBindVariable(tableName),
					sqltypes.StringBindVariable(tableName),
					sqltypes.StringBindVariable("Merge back new table is not needed."),
					sqltypes.StringBindVariable("Merge back new table is not needed."),
					sqltypes.StringBindVariable(dropQuery),
				)
				if err != nil {
					return "", err
				}
			}
			sqlBuf.WriteString(sqlInsertQuery + ";")
		}
		return sqlBuf.String(), nil
	}
	return "", fmt.Errorf("unsupport diffType in GenerateUpdateOrInsertNewTable")
}
func (wr *Wrangler) PrepareMergeBackBranch(ctx context.Context, workflow string, mergeOption string) error {
	sourceSchema, targetSchema, snapshotSchema, branchJob, err := wr.getSchemas(ctx, workflow)

	if branchJob.mergeTimestamp != "" {
		wr.Logger().Printf("branch workflow %s has been merged back at %s, can not be merged again\n",
			workflow, branchJob.mergeTimestamp)
		return nil
	}

	if err != nil {
		return err
	}

	// we set all table entries' need_merge_back field of this workflow to false,
	// so that if user run PrepareMergeBackBranch more than once,
	// it can still work correctly with latest schema.
	alias, err := wr.GetPrimaryTabletAlias(ctx, sidecardb.DefaultCellName)
	if err != nil {
		return err
	}
	sqlUpdateWorkflowNeedMergeBackFalse, err := sqlparser.ParseAndBind(UpdateWorkflowNeedMergeBackFalse,
		sqltypes.StringBindVariable(branchJob.workflowName))
	if err != nil {
		return err
	}
	_, err = wr.ExecuteFetchAsDba(ctx, alias, sqlUpdateWorkflowNeedMergeBackFalse, 1, false, false)
	if err != nil {
		return err
	}

	if mergeOption == MergeOptionOverride {
		analyseAndStoreDiffErr := wr.analyseAndStoreDiff(ctx, branchJob, sourceSchema, targetSchema)
		if analyseAndStoreDiffErr != nil {
			return err
		}
	} else if mergeOption == MergeOptionDiff {
		sourceSchemaForConflict, err := transformSchemaDefinitionToSchema(sourceSchema)
		if err != nil {
			return err
		}
		targetSchemaForConflict, err := transformSchemaDefinitionToSchema(targetSchema)
		if err != nil {
			return err
		}
		snapshotSchemaForConflict, err := transformSchemaDefinitionToSchema(snapshotSchema)
		if err != nil {
			return err
		}

		conflict, conflictMessage, schemasConflictErr := SchemasConflict(sourceSchemaForConflict, targetSchemaForConflict, snapshotSchemaForConflict)
		if schemasConflictErr != nil {
			return schemasConflictErr
		}
		if !conflict {
			// start merge back branch will apply diff on sourceSchema,
			// so here we should calculate diff which diff(snapshotSchema) == targetSchema, so diff(sourceSchema) == mergedSchema
			analyseAndStoreDiffErr := wr.analyseAndStoreDiff(ctx, branchJob, snapshotSchema, targetSchema)
			if analyseAndStoreDiffErr != nil {
				return err
			}
		} else {
			wr.Logger().Printf("PrepareMergeBack (mergeOption=%s) %v conflict:\n", mergeOption, workflow)
			wr.Logger().Printf("%s", conflictMessage)
			return nil
		}
	} else {
		return fmt.Errorf("invalid merge_option %s", mergeOption)
	}

	wr.Logger().Printf("PrepareMergeBack (mergeOption=%s) %v successfully \n", mergeOption, workflow)
	return nil
}

// diff(schema1) == schema2
// merge_ddl will be stored with need_merge_back = 1
func (wr *Wrangler) analyseAndStoreDiff(ctx context.Context, branchJob *BranchJob, schema1, schema2 *tabletmanagerdatapb.SchemaDefinition) error {
	diffEntries, err := analyzeDiffSchema(schema1, schema2)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	sqlBuf := strings.Builder{}
	logBuf := strings.Builder{}
	for _, entry := range diffEntries {
		switch entryStatement := entry.Statement().(type) {
		case *sqlparser.CreateTable:
			// add element into
			tableName := entryStatement.Table.Name.String()
			sqlInsertQuery, err := wr.GenerateUpdateOrInsertNewTable(ctx, entry, branchJob)
			if err != nil {
				return fmt.Errorf("error preparing query: %v", err)
			}
			sqlBuf.WriteString(sqlInsertQuery)
			sqlBuf.WriteString("\n")
			logBuf.WriteString(fmt.Sprintf("extratable: %v entry: %v\n", tableName, entry.CanonicalStatementString()))
		case *sqlparser.CreateView:
			tableName := entryStatement.ViewName.Name.String()
			sqlInsertQuery, err := wr.GenerateUpdateOrInsertNewTable(ctx, entry, branchJob)
			if err != nil {
				return fmt.Errorf("error preparing query: %v", err)
			}
			sqlBuf.WriteString(sqlInsertQuery)
			sqlBuf.WriteString("\n")
			logBuf.WriteString(fmt.Sprintf("extratable: %v entry: %v\n", tableName, entry.CanonicalStatementString()))
		case *sqlparser.AlterTable:
			tableName := entryStatement.Table.Name.String()
			sql := entry.CanonicalStatementString()
			sqlUpdateQuery, err := sqlparser.ParseAndBind(UpdateMergeDDLTemplate,
				sqltypes.StringBindVariable(sql),
				sqltypes.StringBindVariable(branchJob.workflowName),
				sqltypes.StringBindVariable(tableName),
			)
			if err != nil {
				return fmt.Errorf("error preparing query: %v", err)
			}
			sqlBuf.WriteString(sqlUpdateQuery)
			sqlBuf.WriteString("\n")
			logBuf.WriteString(fmt.Sprintf("table: %v entry: %v\n", tableName, sql))
		case *sqlparser.AlterView:
			ViewName := entryStatement.ViewName.Name.String()
			sql := entry.CanonicalStatementString()
			sqlUpdateQuery, err := sqlparser.ParseAndBind(UpdateMergeDDLTemplate,
				sqltypes.StringBindVariable(sql),
				sqltypes.StringBindVariable(branchJob.workflowName),
				sqltypes.StringBindVariable(ViewName),
			)
			if err != nil {
				return fmt.Errorf("error preparing query: %v", err)
			}
			sqlBuf.WriteString(sqlUpdateQuery)
			sqlBuf.WriteString("\n")
			logBuf.WriteString(fmt.Sprintf("table: %v entry: %v\n", ViewName, sql))
		case *sqlparser.DropTable:
			tableName := entryStatement.FromTables[0].Name
			entryStatement.IfExists = true
			sqlInsertQuery, err := wr.GenerateUpdateOrInsertNewTable(ctx, entry, branchJob)
			if err != nil {
				return fmt.Errorf("error preparing query: %v", err)
			}
			sqlBuf.WriteString(sqlInsertQuery)
			sqlBuf.WriteString("\n")
			logBuf.WriteString(fmt.Sprintf("dropTable: %v entry: %v\n", tableName, entry.CanonicalStatementString()))
		default:
			return fmt.Errorf("unsupport diff entry %v", entry)
		}
	}
	alias, err := wr.GetPrimaryTabletAlias(ctx, branchJob.cells)
	if err != nil {
		return err
	}
	sql := sqlBuf.String()
	if sql != "" {
		_, err = wr.ExecuteFetchAsDba(ctx, alias, sql, 1, false, false)
		if err != nil {
			return err
		}
	}
	wr.Logger().Printf("%v", logBuf.String())
	return nil
}

func (wr *Wrangler) StartMergeBackBranch(ctx context.Context, workflow string) error {
	branchJob, err := GetBranchJobByWorkflow(ctx, workflow, wr)
	var vtctld vtctlservicepb.VtctldServer
	if branchJob.externalCluster != "" {
		externalTopo, err := wr.ts.OpenExternalVitessClusterServer(ctx, branchJob.externalCluster)
		if err != nil {
			return err
		}
		wr.sourceTs = externalTopo
		vtctld = grpcvtctldserver.NewVtctldServer(wr.sourceTs)
		log.Infof("Successfully opened external topo: %+v", externalTopo)
	}
	if vtctld == nil {
		vtctld = wr.VtctldServer()
	}
	if err != nil {
		return err
	}
	var parts []string
	for _, tableRules := range branchJob.bs.FilterTableRules {
		if tableRules.MergeDdl == "copy" || !tableRules.NeedMergeBack {
			continue
		}
		parts = append(parts, tableRules.MergeDdl)
	}
	var resp *vtctldatapb.ApplySchemaResponse
	if len(parts) != 0 {
		resp, err = vtctld.ApplySchema(ctx, &vtctldatapb.ApplySchemaRequest{
			Keyspace:                branchJob.sourceDatabase,
			AllowLongUnavailability: false,
			DdlStrategy:             "online",
			Sql:                     parts,
			SkipPreflight:           true,
			WaitReplicasTimeout:     protoutil.DurationToProto(DefaultWaitReplicasTimeout),
		})
		if err != nil {
			wr.Logger().Errorf("%s\n", err.Error())
			return err
		}
	}
	if resp != nil {
		alias, err := wr.GetPrimaryTabletAlias(ctx, branchJob.cells)
		if err != nil {
			return err
		}
		_, err = wr.ExecuteFetchAsDba(ctx, alias, "START TRANSACTION", 1, false, false)
		if err != nil {
			return err
		}
		index := 0
		for _, tableRules := range branchJob.bs.FilterTableRules {
			if tableRules.MergeDdl == "copy" || !tableRules.NeedMergeBack {
				continue
			}
			sql := fmt.Sprintf("UPDATE mysql.branch_table_rules set merge_ddl_uuid='%s' where id=%d", resp.UuidList[index], tableRules.Id)
			_, err = wr.ExecuteFetchAsDba(ctx, alias, sql, 1, false, false)
			if err != nil {
				return err
			}
			index++
		}

		sqlUpdateWorkflowNeedMergeBackFalse, err := sqlparser.ParseAndBind(UpdateWorkflowNeedMergeBackFalse,
			sqltypes.StringBindVariable(branchJob.workflowName))
		if err != nil {
			return err
		}
		_, err = wr.ExecuteFetchAsDba(ctx, alias, sqlUpdateWorkflowNeedMergeBackFalse, 1, false, false)
		if err != nil {
			return err
		}

		sqlUpdateWorkflowMergeTimestamp, err := sqlparser.ParseAndBind(UpdateWorkflowMergeTimestamp,
			sqltypes.StringBindVariable(time.Now().Format(time.RFC3339)),
			sqltypes.StringBindVariable(branchJob.workflowName))
		if err != nil {
			return err
		}
		_, err = wr.ExecuteFetchAsDba(ctx, alias, sqlUpdateWorkflowMergeTimestamp, 1, false, false)
		if err != nil {
			return err
		}
		_, err = wr.ExecuteFetchAsDba(ctx, alias, "COMMIT", 1, false, false)
		if err != nil {
			return err
		}
	}
	wr.Logger().Printf("Start mergeBack %v successfully. uuid list:\n", workflow)
	if resp != nil {
		for _, uuid := range resp.UuidList {
			wr.Logger().Printf("[%s]\n", uuid)
		}
	}
	return nil
}

func (wr *Wrangler) CleanupBranch(ctx context.Context, workflow string) error {
	branchJob, err := GetBranchJobByWorkflow(ctx, workflow, wr)
	if err != nil {
		return err
	}
	alias, err := wr.GetPrimaryTabletAlias(ctx, "zone1")
	if err != nil {
		return err
	}
	_, err = wr.ExecuteFetchAsDba(ctx, alias, "START TRANSACTION", 1, false, false)
	if err != nil {
		return err
	}
	deleteTableRules := fmt.Sprintf(DeleteBranchTableRuleByWorkflow, branchJob.workflowName)
	_, err = wr.ExecuteFetchAsDba(ctx, alias, deleteTableRules, 1, false, false)
	if err != nil {
		return err
	}
	deleteBranchJob := fmt.Sprintf(DeleteBranchJobByWorkflow, branchJob.workflowName)
	_, err = wr.ExecuteFetchAsDba(ctx, alias, deleteBranchJob, 1, false, false)
	if err != nil {
		return err
	}
	deleteBranchSnapshot := fmt.Sprintf(DeleteBranchSnapshotByWorkflow, branchJob.workflowName)
	_, err = wr.ExecuteFetchAsDba(ctx, alias, deleteBranchSnapshot, 1, false, false)
	if err != nil {
		return err
	}
	// todo: delete Vreplication and delete branch jobs should be atomic.
	for _, tableRule := range branchJob.bs.FilterTableRules {
		if tableRule.MergeDdlUuid != "" {
			deleteVReplication := fmt.Sprintf(DeleteVReplicationByWorkFlow, tableRule.MergeDdlUuid)
			_, err := wr.VReplicationExec(ctx, alias, deleteVReplication)
			if err != nil {
				return err
			}
		}
	}
	deleteVReplication := fmt.Sprintf(DeleteVReplicationByWorkFlow, branchJob.workflowName)
	_, err = wr.ExecuteFetchAsDba(ctx, alias, deleteVReplication, 1, false, false)
	if err != nil {
		return err
	}
	_, err = wr.ExecuteFetchAsDba(ctx, alias, "COMMIT", 1, false, false)
	if err != nil {
		return err
	}
	wr.Logger().Printf("cleanup workflow:%v successfully\n", branchJob.workflowName)
	return nil
	// delete from branch_table_rules
}

func (wr *Wrangler) SchemaDiff(ctx context.Context, workflow, outputTypeFlag, compareObjectsFlag string) error {
	sourceSchema, targetSchema, snapshotSchema, branchJob, err := wr.getSchemas(ctx, workflow)
	sourceDatabase := branchJob.sourceDatabase
	targetDatabase := branchJob.targetDatabase
	if err != nil {
		return err
	}

	switch outputTypeFlag {
	case OutputTypeCreateTable:
		return wr.analyseSchemaDiffAndOutputCreateTable(targetSchema, sourceSchema, snapshotSchema, targetDatabase, sourceDatabase, compareObjectsFlag)
	case OutputTypeDDL:
		return wr.analyseSchemaDiffAndOutputDDL(targetSchema, sourceSchema, snapshotSchema, compareObjectsFlag)
	case OutputTypeConflict:
		return wr.analyseSchemaDiffAndOutputConflict(targetSchema, sourceSchema, snapshotSchema, compareObjectsFlag)
	default:
		return fmt.Errorf("%v is invalid output_type flag, should be one of %v, %v, %v",
			outputTypeFlag, OutputTypeCreateTable, OutputTypeDDL, OutputTypeConflict)
	}
}

func (wr *Wrangler) storeSchemaSnapshot(ctx context.Context, workflow string, mz *materializer) error {
	allTables := []string{"/.*/"}
	targetDbName := mz.targetShards[0].Keyspace()
	return mz.forAllTargets(func(target *topo.ShardInfo) error {
		req := &tabletmanagerdatapb.GetSchemaRequest{Tables: allTables, DbName: targetDbName}
		targetSchema, err := schematools.GetSchema(ctx, mz.wr.ts, mz.wr.tmc, target.PrimaryAlias, req)
		if err != nil {
			return err
		}

		targetSchema = filterSchemaRelatedOnlineDDLAndGCTableArtifact(targetSchema)

		schemaBlob, err := proto.Marshal(targetSchema)
		if err != nil {
			return err
		}

		sqlTemplate := "insert into mysql.branch_snapshots (workflow_name, schema_snapshot) values (%a, %a);"
		sql, err := sqlparser.ParseAndBind(sqlTemplate,
			sqltypes.StringBindVariable(workflow),
			sqltypes.StringBindVariable(string(schemaBlob)))
		if err != nil {
			return err
		}

		_, err = wr.ExecuteQueryByPrimary(ctx, sql, true, false)
		if err != nil {
			return err
		}
		return nil
	})
}

func (wr *Wrangler) restoreSchemaSnapshot(ctx context.Context, workflow string) (*tabletmanagerdatapb.SchemaDefinition, error) {
	sqlTemplate := "select schema_snapshot from mysql.branch_snapshots where workflow_name = %a;"
	sql, err := sqlparser.ParseAndBind(sqlTemplate,
		sqltypes.StringBindVariable(workflow))
	if err != nil {
		return nil, err
	}

	qr, err := wr.ExecuteQueryByPrimary(ctx, sql, true, false)
	if err != nil {
		return nil, err
	}

	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("the len of query result of snapshot is not 1 but %v", len(qr.Rows))
	}

	schemaSnapshot := &tabletmanagerdatapb.SchemaDefinition{}
	proto.Unmarshal(qr.Rows[0].Values, schemaSnapshot)
	return schemaSnapshot, nil
}

func (wr *Wrangler) getSourceAndTargetSchema(ctx context.Context, workflow string) (sourceSchema, targetSchema *tabletmanagerdatapb.SchemaDefinition, branchJob *BranchJob, err error) {
	branchJob, err = GetBranchJobByWorkflow(ctx, workflow, wr)
	if err != nil {
		return nil, nil, nil, err
	}
	if branchJob.externalCluster != "" {
		externalTopo, err := wr.ts.OpenExternalVitessClusterServer(ctx, branchJob.externalCluster)
		if err != nil {
			return nil, nil, nil, err
		}
		wr.sourceTs = externalTopo
		log.Infof("Successfully opened external topo: %+v", externalTopo)
	}
	alias, err := wr.GetPrimaryTabletAlias(ctx, sidecardb.DefaultCellName)
	if err != nil {
		return nil, nil, nil, err
	}
	// todo: we don't support blacklist and white list now, so we just get all tables from source and target database
	//var tables []string
	//for _, tableRules := range branchJob.bs.FilterTableRules {
	//	tables = append(tables, tableRules.TargetTable)
	//}
	sourceDatabaseRequest := &tabletmanagerdatapb.GetSchemaRequest{
		//Tables:          tables,
		TableSchemaOnly: true,
		DbName:          branchJob.sourceDatabase,
	}
	targetDatabaseRequest := &tabletmanagerdatapb.GetSchemaRequest{
		//Tables:          tables,
		TableSchemaOnly: true,
		DbName:          branchJob.targetDatabase,
	}

	sourceSchema, err = schematools.GetSchema(ctx, wr.sourceTs, wr.tmc, alias, sourceDatabaseRequest)
	if err != nil {
		return nil, nil, nil, err
	}
	targetSchema, err = schematools.GetSchema(ctx, wr.TopoServer(), wr.tmc, alias, targetDatabaseRequest)
	if err != nil {
		return nil, nil, nil, err
	}

	return sourceSchema, targetSchema, branchJob, nil
}

func (wr *Wrangler) getSchemas(ctx context.Context, workflow string) (sourceSchema, targetSchema, snapshotSchema *tabletmanagerdatapb.SchemaDefinition, branchJob *BranchJob, err error) {
	sourceSchema, targetSchema, branchJob, err = wr.getSourceAndTargetSchema(ctx, workflow)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	snapshotSchema, err = wr.restoreSchemaSnapshot(ctx, workflow)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	targetSchema = filterSchemaRelatedOnlineDDLAndGCTableArtifact(targetSchema)
	sourceSchema = filterSchemaRelatedOnlineDDLAndGCTableArtifact(sourceSchema)
	snapshotSchema = filterSchemaRelatedOnlineDDLAndGCTableArtifact(snapshotSchema)

	return sourceSchema, targetSchema, snapshotSchema, branchJob, nil
}

func (wr *Wrangler) analyseSchemaDiffAndOutputCreateTable(targetSchema, sourceSchema, snapshotSchema *tabletmanagerdatapb.SchemaDefinition, targetDatabase, sourceDatabase, compareObjectsFlag string) error {
	er := concurrency.AllErrorRecorder{}
	var records []tmutils.SchemaDiffElement
	// analyze Schema diff between sourceDatabase and targetDatabase

	switch compareObjectsFlag {
	case CompareObjectsSourceTarget, CompareObjectsTargetSource:
		wr.Logger().Printf("schema diff between sourceDatabase and targetDatabase:\n")
		records = tmutils.DiffSchema(sourceDatabase, sourceSchema, targetDatabase, targetSchema, &er)
		if len(records) == 0 {
			wr.Logger().Printf("schemas are the same\n")
		}
		for _, record := range records {
			wr.Logger().Printf("%v\n", record.Report())
		}
		return nil
	case CompareObjectsTargetSnapshot, CompareObjectsSnapshotTarget:
		// analyze Schema diff between targetDatabase and snapshot
		wr.Logger().Printf("schema diff between targetDatabase and snapshot:\n")
		records = tmutils.DiffSchema(targetDatabase, targetSchema, "snapshot", snapshotSchema, &er)
		if len(records) == 0 {
			wr.Logger().Printf("schemas are the same\n")
		}
		for _, record := range records {
			wr.Logger().Printf("%v\n", record.Report())
		}
		return nil
	case CompareObjectsSnapshotSource, CompareObjectsSourceSnapshot:
		// analyze Schema diff between sourceDatabase and snapshot
		wr.Logger().Printf("schema diff between sourceDatabase and snapshot:\n")
		records = tmutils.DiffSchema(sourceDatabase, sourceSchema, "snapshot", snapshotSchema, &er)
		if len(records) == 0 {
			wr.Logger().Printf("schemas are the same\n")
		}
		for _, record := range records {
			wr.Logger().Printf("%v\n", record.Report())
		}
		return nil
	default:
		return fmt.Errorf("%v is invalid compare_objects flag, should be one of %v, %v, %v, %v, %v, %v",
			compareObjectsFlag,
			CompareObjectsSourceTarget, CompareObjectsTargetSource,
			CompareObjectsTargetSnapshot, CompareObjectsSnapshotTarget,
			CompareObjectsSnapshotSource, CompareObjectsSourceSnapshot)
	}
}

func (wr *Wrangler) analyseSchemaDiffAndOutputDDL(targetSchema, sourceSchema, snapshotSchema *tabletmanagerdatapb.SchemaDefinition, compareObjectsFlag string) error {
	var entityDiffs []schemadiff.EntityDiff
	var err error
	switch compareObjectsFlag {
	case CompareObjectsSourceTarget:
		wr.Logger().Printf("The DDLs required to transform from the source schema to the target schema are as follows:\n")
		entityDiffs, err = analyzeDiffSchema(sourceSchema, targetSchema)
		if err != nil {
			return err
		}
	case CompareObjectsTargetSource:
		wr.Logger().Printf("The DDLs required to transform from the target schema to the source schema are as follows:\n")
		entityDiffs, err = analyzeDiffSchema(targetSchema, sourceSchema)
		if err != nil {
			return err
		}
	case CompareObjectsTargetSnapshot:
		wr.Logger().Printf("The DDLs required to transform from the target schema to the snapshot schema are as follows:\n")
		entityDiffs, err = analyzeDiffSchema(targetSchema, snapshotSchema)
		if err != nil {
			return err
		}
	case CompareObjectsSnapshotTarget:
		wr.Logger().Printf("The DDLs required to transform from the snapshot schema to the target schema are as follows:\n")
		entityDiffs, err = analyzeDiffSchema(snapshotSchema, targetSchema)
		if err != nil {
			return err
		}
	case CompareObjectsSnapshotSource:
		wr.Logger().Printf("The DDLs required to transform from the snapshot schema to the source schema are as follows:\n")
		entityDiffs, err = analyzeDiffSchema(snapshotSchema, sourceSchema)
		if err != nil {
			return err
		}
	case CompareObjectsSourceSnapshot:
		wr.Logger().Printf("The DDLs required to transform from the source schema to the snapshot schema are as follows:\n")
		entityDiffs, err = analyzeDiffSchema(sourceSchema, snapshotSchema)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("%v is invalid compare_objects flag, should be one of %v, %v, %v, %v, %v, %v",
			compareObjectsFlag,
			CompareObjectsSourceTarget, CompareObjectsTargetSource,
			CompareObjectsTargetSnapshot, CompareObjectsSnapshotTarget,
			CompareObjectsSnapshotSource, CompareObjectsSourceSnapshot)
	}
	if len(entityDiffs) == 0 {
		wr.Logger().Printf("the schemas are the same\n")
	} else {
		for _, entityDiff := range entityDiffs {
			wr.Logger().Printf("entity %v\n", entityDiff.StatementString())
		}
	}
	return nil
}

func (wr *Wrangler) analyseSchemaDiffAndOutputConflict(targetSchemaDefinition, sourceSchemaDefinition, snapshotSchemaDefinition *tabletmanagerdatapb.SchemaDefinition, compareObjectsFlag string) error {
	switch compareObjectsFlag {
	case CompareObjectsSourceTarget, CompareObjectsTargetSource:
		sourceSchema, err := transformSchemaDefinitionToSchema(sourceSchemaDefinition)
		if err != nil {
			return err
		}
		targetSchema, err := transformSchemaDefinitionToSchema(targetSchemaDefinition)
		if err != nil {
			return err
		}
		snapshotSchema, err := transformSchemaDefinitionToSchema(snapshotSchemaDefinition)
		if err != nil {
			return err
		}

		conflict, conflictMessage, err := SchemasConflict(sourceSchema, targetSchema, snapshotSchema)
		if err != nil {
			return err
		}
		if conflict {
			wr.Logger().Printf("the source and target schema conflict:\n%v", conflictMessage)
		} else {
			wr.Logger().Printf("the source and target schema are not in conflict\n")
		}
		return nil
	case CompareObjectsTargetSnapshot, CompareObjectsSnapshotTarget, CompareObjectsSnapshotSource, CompareObjectsSourceSnapshot:
		wr.Logger().Printf("the source and target are both derived from snapshot, so there is no conflicts\n")
		return nil

	default:
		return fmt.Errorf("%v is invalid compare_objects flag, should be one of %v, %v, %v, %v, %v, %v",
			compareObjectsFlag,
			CompareObjectsSourceTarget, CompareObjectsTargetSource,
			CompareObjectsTargetSnapshot, CompareObjectsSnapshotTarget,
			CompareObjectsSnapshotSource, CompareObjectsSourceSnapshot)
	}
}

// this function check if the schema1Str and schema2 ( which are both derived from the snapshot schema) conflict using "three-way merge algorithm",
// the algorithm is described as follows:
// 1. Calculate the difference between main and schema1Str, view diff1 as a function: diff1(main) => schema1Str.
// 2. Calculate the difference between main and schema2, diff2: diff2(main) => schema2.
// 3. Apply diff1 on diff2(main), i.e., diff1(diff2(main)). If it's invalid, there's a conflict.
// 4. Apply diff2 on diff1(main), i.e., diff2(diff1(main)). If it's invalid, there's a conflict.
// 5. If both are valid but diff1(diff2(main)) is not equal to diff2(diff1(main)), there's a conflict.
// 6. If both transformations are valid, and diff1(diff2(main)) equals diff2(diff1(main)), it signifies there's no conflict.
func SchemasConflict(schema1, schema2, snapshotSchema *schemadiff.Schema) (bool, string, error) {
	diffFromSnapshotToSchema1, err := getSchemaDiff(snapshotSchema, schema1)
	if err != nil {
		return true, "", err
	}

	diffFromSnapshotToSchema2, err := getSchemaDiff(snapshotSchema, schema2)
	if err != nil {
		return true, "", err
	}

	// apply diffFromSnapshotToSchema2 on schema1Str, check whether is valid
	diffFromSnapshotToSchema2OnSchema1, err := schema1.Apply(diffFromSnapshotToSchema2)
	if err != nil {
		// we don't care about the content of error, it means we can't apply diffFromSnapshotToSchema2 on schema1Str
		ddlStr := ""
		for _, ddl := range diffFromSnapshotToSchema2 {
			ddlStr += fmt.Sprintf("%s\n", ddl.StatementString())
		}
		return true, err.Error() + fmt.Sprintf(" when apply\n%son\n%s", ddlStr, schema1.ToSQL()), nil
	}

	// apply diffFromSnapshotToSchema1 on schema2, check whether is valid
	diffFromSnapshotToSchema1OnSchema2, err := schema2.Apply(diffFromSnapshotToSchema1)
	if err != nil {
		// we don't care about the content of error, it means we can't apply diffFromSnapshotToSchema1 on schema2
		ddlStr := ""
		for _, ddl := range diffFromSnapshotToSchema1 {
			ddlStr += fmt.Sprintf("%s\n", ddl.StatementString())
		}
		return true, err.Error() + fmt.Sprintf(" when apply\n%son\n%s", ddlStr, schema2.ToSQL()), nil
	}

	if diffFromSnapshotToSchema1OnSchema2.ToSQL() != diffFromSnapshotToSchema2OnSchema1.ToSQL() {
		return true,
			fmt.Sprintf("diff1(diff2(snapshot)) != diff2(diff1(snapshot)):\n--------------\n%s--------------\ndosn't equals to\n--------------\n%s--------------\n", diffFromSnapshotToSchema1OnSchema2.ToSQL(), diffFromSnapshotToSchema2OnSchema1.ToSQL()),
			nil
	}

	return false, "", nil
}

func filterSchemaRelatedOnlineDDLAndGCTableArtifact(originalSchema *tabletmanagerdatapb.SchemaDefinition) *tabletmanagerdatapb.SchemaDefinition {
	var filteredSchemaTableDefinition []*tabletmanagerdatapb.TableDefinition
	for _, table := range originalSchema.TableDefinitions {
		if schema.IsOnlineDDLTableName(table.Name) || schema.IsGCTableName(table.Name) {
			continue
		}
		filteredSchemaTableDefinition = append(filteredSchemaTableDefinition, table)
	}
	originalSchema.TableDefinitions = filteredSchemaTableDefinition
	return originalSchema
}

func transformSchemaDefinitionToSchema(schemaDefinition *tabletmanagerdatapb.SchemaDefinition) (*schemadiff.Schema, error) {
	var schemaQueries []string
	for _, table := range schemaDefinition.TableDefinitions {
		schemaQueries = append(schemaQueries, table.Schema)
	}
	schema, err := schemadiff.NewSchemaFromQueries(schemaQueries)
	if err != nil {
		return nil, err
	}
	return schema, nil
}
