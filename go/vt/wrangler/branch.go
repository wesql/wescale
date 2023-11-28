/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package wrangler

import (
	"context"
	"fmt"
	"strings"

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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type BranchJob struct {
	wr               *Wrangler
	bs               *vtctldatapb.BranchSettings
	sourceDatabase   string
	targetDatabase   string
	workflowName     string
	sourceTopo       string
	sourceTabletType string
	includeTables    string
	excludeTables    string
	cells            string
	skipCopyPhase    bool
	stopAfterCopy    bool
	onddl            string
	status           string
}

const (
	BranchStatusOfPrePare = "Prepare"
	BranchStatusOfRunning = "Running"
)
const SelectBranchJobByWorkflow = "select * from mysql.branch_jobs where workflow_name = '%s'"

const SelectBranchTableRuleByWorkflow = "select * from mysql.branch_table_rules where workflow_name = '%s'"

const DeleteBranchJobByWorkflow = "DELETE FROM mysql.branch_jobs where workflow_name='%s'"

const DeleteBranchTableRuleByWorkflow = "DELETE FROM mysql.branch_table_rules where workflow_name='%s'"

const DeleteVReplicationByWorkFlow = "DELETE FROM mysql.vreplication where workflow='%s'"

func (branchJob *BranchJob) generateInsert() (string, error) {
	buf := &strings.Builder{}
	buf.WriteString("INSERT INTO mysql.branch_jobs (source_database, target_database, workflow_name, source_topo, source_tablet_type, cells, skip_copy_phase, stop_after_copy, onddl, status, message) VALUES ")
	buf.WriteString(fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, '%s', '%s', '%s')",
		branchJob.sourceDatabase,
		branchJob.targetDatabase,
		branchJob.workflowName,
		branchJob.sourceTopo,
		branchJob.sourceTabletType,
		branchJob.cells,
		boolToInt(branchJob.skipCopyPhase),
		boolToInt(branchJob.stopAfterCopy),
		branchJob.onddl,
		branchJob.status,
		""))
	return buf.String(), nil
}

// Helper function to convert bool to int (0 or 1) for tinyint fields
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (branchJob *BranchJob) generateRulesInsert() (string, error) {
	buf := &strings.Builder{}
	buf.WriteString("INSERT INTO mysql.branch_table_rules (workflow_name, source_table_name, target_table_name, filtering_rule, create_ddl, merge_ddl,default_filter_rules) VALUES")
	first := true
	for _, tableRule := range branchJob.bs.FilterTableRules {
		if first {
			first = false
		} else {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("('%v','%v','%v','%v','%v','%v','%v')", branchJob.workflowName, tableRule.SourceTable, tableRule.TargetTable, tableRule.FilteringRule, tableRule.CreateDdl, tableRule.MergeDdl, tableRule.DefaultFilterRules))
	}
	buf.WriteString(";")
	return buf.String(), nil
}

// PrepareBranch should insert BranchSettings data into mysql.branch_setting
func (wr *Wrangler) PrepareBranch(ctx context.Context, workflow, sourceDatabase, targetDatabase,
	cell, tabletTypes string, includeTables, excludeTables string, stopAfterCopy bool, defaultFilterRules string) error {
	err := wr.CreateDatabase(ctx, targetDatabase)
	if err != nil {
		return err
	}
	branchJob := &BranchJob{
		status:           BranchStatusOfPrePare,
		workflowName:     workflow,
		sourceDatabase:   sourceDatabase,
		targetDatabase:   targetDatabase,
		sourceTabletType: tabletTypes,
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
	var tables []string
	var vschema *vschemapb.Keyspace
	vschema, err = wr.ts.GetVSchema(ctx, targetDatabase)
	if err != nil {
		return err
	}
	if vschema == nil {
		return fmt.Errorf("no vschema found for target keyspace %s", targetDatabase)
	}
	// get source keyspace tables
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
	//generate filterTableRule
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
		}
		branchJob.bs.FilterTableRules = append(branchJob.bs.FilterTableRules, filterTableRule)
	}
	//get insert filterTableRule sql
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
	alias, err := wr.GetPrimaryTabletAlias(ctx, "zone1")
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(SelectBranchJobByWorkflow, workflow)
	result, err := wr.ExecuteFetchAsApp(ctx, alias, true, sql, 1)
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
	if err != nil {
		return nil, err
	}
	onddl := branchJobMap["onddl"].ToString()

	status := branchJobMap["status"].ToString()
	if status != BranchStatusOfPrePare {
		return nil, vterrors.Errorf(vtrpc.Code_ABORTED, "can not start an branch which status [%v] is not prepare", status)
	}
	branchJob := &BranchJob{
		sourceDatabase:   sourceDatabase,
		targetDatabase:   targetDatabase,
		workflowName:     workflow,
		sourceTopo:       sourceTopo,
		sourceTabletType: sourceTabletType,
		stopAfterCopy:    stopAfterCopy,
		onddl:            onddl,
		cells:            cells,
		status:           status,
	}
	branchJob.bs, err = GetBranchTableRulesByWorkflow(ctx, workflow, wr)
	if err != nil {
		return nil, err
	}
	return branchJob, nil
}

func GetBranchTableRulesByWorkflow(ctx context.Context, workflow string, wr *Wrangler) (*vtctldatapb.BranchSettings, error) {
	bs := &vtctldatapb.BranchSettings{}
	alias, err := wr.GetPrimaryTabletAlias(ctx, "zone1")
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(SelectBranchTableRuleByWorkflow, workflow)
	result, err := wr.ExecuteFetchAsApp(ctx, alias, true, sql, 1000)
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
		fileterRule := &vtctldatapb.FilterTableRule{
			Id:            id,
			SourceTable:   sourceTableName,
			TargetTable:   targetTableName,
			FilteringRule: filterRule,
			CreateDdl:     createDDL,
			MergeDdl:      mergeDDL,
			NeedMergeBack: needMergeBack,
			MergeDdlUuid:  mergeUUID,
		}
		bs.FilterTableRules = append(bs.FilterTableRules, fileterRule)
	}
	return bs, nil

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
		StopAfterCopy:         true,
		ExternalCluster:       "",
	}
	for _, rule := range branchJob.bs.FilterTableRules {
		ts := &vtctldatapb.TableMaterializeSettings{
			TargetTable:      rule.TargetTable,
			SourceExpression: rule.FilteringRule,
			CreateDdl:        rule.CreateDdl,
		}
		ms.TableSettings = append(ms.TableSettings, ts)
	}
	return ms, nil
}

func (wr *Wrangler) StreamExist(ctx context.Context, workflow string) (bool, error) {
	sql := fmt.Sprintf("SELECT 1 FROM mysql.vreplication WHERE workflow='%s';", workflow)
	tabletAliases, err := wr.GetPrimaryTabletAlias(ctx, "zone1")
	if err != nil {
		return false, err
	}
	result, err := wr.ExecuteFetchAsApp(ctx, tabletAliases, true, sql, 1)
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
	} else {
		mz, err = wr.buildMaterializer(ctx, ms)
		if err != nil {
			return err
		}
	}
	err = mz.startStreams(ctx)
	if err != nil {
		return err
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
	err = mz.stopStreams(ctx)
	if err != nil {
		return err
	}
	wr.Logger().Printf("Start workflow %v successfully", workflow)
	return nil
}
func (wr *Wrangler) PrepareMergeBackBranch(ctx context.Context, workflow string) error {
	branchJob, err := GetBranchJobByWorkflow(ctx, workflow, wr)
	if err != nil {
		return err
	}
	alias, err := wr.GetPrimaryTabletAlias(ctx, "zone1")
	if err != nil {
		return err
	}
	var tables []string
	for _, tableRules := range branchJob.bs.FilterTableRules {
		tables = append(tables, tableRules.TargetTable)
	}
	sourceDatabaseReqeust := &tabletmanagerdatapb.GetSchemaRequest{
		Tables:          tables,
		TableSchemaOnly: true,
		DbName:          branchJob.sourceDatabase,
	}
	targetDatabaseReqeust := &tabletmanagerdatapb.GetSchemaRequest{
		//Tables:          tables,
		TableSchemaOnly: true,
		DbName:          branchJob.targetDatabase,
	}
	er := concurrency.AllErrorRecorder{}

	// analyze Schema diff between sourceDatabase and targetDatabase
	targetSchema, err := schematools.GetSchema(ctx, wr.TopoServer(), wr.tmc, alias, targetDatabaseReqeust)
	if err != nil {
		return err
	}
	sourceSchema, err := schematools.GetSchema(ctx, wr.TopoServer(), wr.tmc, alias, sourceDatabaseReqeust)
	if err != nil {
		return err
	}
	record := tmutils.DiffSchema(targetDatabaseReqeust.DbName, targetSchema, sourceDatabaseReqeust.DbName, sourceSchema, &er)
	ddls, err := tmutils.AnalyzeDiffRecord(record, sourceDatabaseReqeust.DbName, targetDatabaseReqeust.DbName, sourceSchema, targetSchema)
	if err != nil {
		return err
	}
	sqlBuf := strings.Builder{}
	logBuf := strings.Builder{}
	for _, ddl := range ddls {
		switch ddl.DiffType {
		case tmutils.ExtraTable:
			// add element into
			tableName := ddl.TableName
			sqlBuf.WriteString("INSERT INTO mysql.branch_table_rules (workflow_name, source_table_name, target_table_name, filtering_rule, create_ddl, merge_ddl) VALUES")
			sqlBuf.WriteString(fmt.Sprintf("('%v','%v','%v','%v','%v','%v');", branchJob.workflowName, tableName, tableName, "Merge back new table is not needed.", "Merge back new table is not needed.", ddl.Ddl))
			sqlBuf.WriteString("\n")
			logBuf.WriteString(fmt.Sprintf("extratable: %v ddl: %v\n", tableName, ddl.Ddl))
		case tmutils.TableSchemaDiff, tmutils.TableTypeDiff:
			sqlBuf.WriteString(fmt.Sprintf("UPDATE mysql.branch_table_rules set merge_ddl='%v' where workflow_name='%v' and target_table_name='%v';", ddl.Ddl, branchJob.workflowName, ddl.TableName))
			sqlBuf.WriteString("\n")
			logBuf.WriteString(fmt.Sprintf("table: %v ddl: %v\n", ddl.TableName, ddl.Ddl))
		}
	}
	alias, err = wr.GetPrimaryTabletAlias(ctx, branchJob.cells)
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
	wr.Logger().Printf("PrepareMergeBack %v successfully \n", workflow)
	wr.Logger().Printf("%v", logBuf.String())
	return nil
}

func (wr *Wrangler) StartMergeBackBranch(ctx context.Context, workflow string) error {
	branchJob, err := GetBranchJobByWorkflow(ctx, workflow, wr)
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
		resp, err = wr.VtctldServer().ApplySchema(ctx, &vtctldatapb.ApplySchemaRequest{
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
	for _, tableRule := range branchJob.bs.FilterTableRules {
		if tableRule.MergeDdlUuid != "" {
			deleteVReplication := fmt.Sprintf(DeleteVReplicationByWorkFlow, tableRule.MergeDdlUuid)
			_, err = wr.ExecuteFetchAsDba(ctx, alias, deleteVReplication, 1, false, false)
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
	wr.Logger().Printf("cleanup workflow:%v successfully", branchJob.workflowName)
	return nil
	// delete from branch_table_rules
}
