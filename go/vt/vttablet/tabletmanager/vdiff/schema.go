/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vdiff

const (
	sqlNewVDiff    = "insert into mysql.vdiff(keyspace, workflow, state, options, shard, db_name, vdiff_uuid) values(%s, %s, '%s', %s, '%s', '%s', '%s')"
	sqlResumeVDiff = `update mysql.vdiff as vd, mysql.vdiff_table as vdt set vd.options = %s, vd.started_at = NULL, vd.completed_at = NULL, vd.state = 'pending',
					vdt.state = 'pending' where vd.vdiff_uuid = %s and vd.id = vdt.vdiff_id and vd.state in ('completed', 'stopped')
					and vdt.state in ('completed', 'stopped')`
	sqlRetryVDiff = `update mysql.vdiff as vd left join mysql.vdiff_table as vdt on (vd.id = vdt.vdiff_id) set vd.state = 'pending',
					vd.last_error = '', vdt.state = 'pending' where vd.id = %d and (vd.state = 'error' or vdt.state = 'error')`
	sqlGetVDiffByKeyspaceWorkflowUUID = "select * from mysql.vdiff where keyspace = %s and workflow = %s and vdiff_uuid = %s"
	sqlGetMostRecentVDiff             = "select * from mysql.vdiff where keyspace = %s and workflow = %s order by id desc limit 1"
	sqlGetVDiffByID                   = "select * from mysql.vdiff where id = %d"
	sqlDeleteVDiffs                   = `delete from vd, vdt, vdl using mysql.vdiff as vd left join mysql.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
										left join mysql.vdiff_log as vdl on (vd.id = vdl.vdiff_id)
										where vd.keyspace = %s and vd.workflow = %s`
	sqlDeleteVDiffByUUID = `delete from vd, vdt using mysql.vdiff as vd left join mysql.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
							and vd.vdiff_uuid = %s`
	sqlVDiffSummary = `select vd.state as vdiff_state, vd.last_error as last_error, vdt.table_name as table_name,
						vd.vdiff_uuid as 'uuid', vdt.state as table_state, vdt.table_rows as table_rows,
						vd.started_at as started_at, vdt.rows_compared as rows_compared, vd.completed_at as completed_at,
						IF(vdt.mismatch = 1, 1, 0) as has_mismatch, vdt.report as report
						from mysql.vdiff as vd left join mysql.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vd.id = %d`
	// sqlUpdateVDiffState has a penultimate placeholder for any additional columns you want to update, e.g. `, foo = 1`
	sqlUpdateVDiffState   = "update mysql.vdiff set state = %s, last_error = %s %s where id = %d"
	sqlUpdateVDiffStopped = `update mysql.vdiff as vd, mysql.vdiff_table as vdt set vd.state = 'stopped', vdt.state = 'stopped', vd.last_error = ''
							where vd.id = vdt.vdiff_id and vd.id = %d and vd.state != 'completed'`
	sqlGetVReplicationEntry = "select * from mysql.vreplication %s"
	sqlGetVDiffsToRun       = "select * from mysql.vdiff where state in ('started','pending')" // what VDiffs have not been stopped or completed
	sqlGetVDiffsToRetry     = "select * from mysql.vdiff where state = 'error' and json_unquote(json_extract(options, '$.core_options.auto_retry')) = 'true'"
	sqlGetVDiffID           = "select id as id from mysql.vdiff where vdiff_uuid = %s"
	sqlGetAllVDiffs         = "select * from mysql.vdiff order by id desc"
	sqlGetAllTableRows      = "select table_name as table_name, table_rows as table_rows from INFORMATION_SCHEMA.TABLES where table_schema = %s and table_name in (%s)"

	sqlNewVDiffTable = "insert into mysql.vdiff_table(vdiff_id, table_name, state, table_rows) values(%d, %s, 'pending', %d)"
	sqlGetVDiffTable = `select vdt.lastpk as lastpk, vdt.mismatch as mismatch, vdt.report as report
						from mysql.vdiff as vd inner join mysql.vdiff_table as vdt on (vd.id = vdt.vdiff_id)
						where vdt.vdiff_id = %d and vdt.table_name = %s`
	sqlUpdateTableRows           = "update mysql.vdiff_table set table_rows = %d where vdiff_id = %d and table_name = %s"
	sqlUpdateTableProgress       = "update mysql.vdiff_table set rows_compared = %d, lastpk = %s, report = %s where vdiff_id = %d and table_name = %s"
	sqlUpdateTableNoProgress     = "update mysql.vdiff_table set rows_compared = %d, report = %s where vdiff_id = %d and table_name = %s"
	sqlUpdateTableState          = "update mysql.vdiff_table set state = %s where vdiff_id = %d and table_name = %s"
	sqlUpdateTableStateAndReport = "update mysql.vdiff_table set state = %s, rows_compared = %d, report = %s where vdiff_id = %d and table_name = %s"
	sqlUpdateTableMismatch       = "update mysql.vdiff_table set mismatch = true where vdiff_id = %d and table_name = %s"

	sqlGetIncompleteTables = "select table_name as table_name from mysql.vdiff_table where vdiff_id = %d and state != 'completed'"
)
