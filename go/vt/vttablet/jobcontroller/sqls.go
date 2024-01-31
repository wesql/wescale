/*
Copyright ApeCloud, Inc.
Licensed under the Apache v2(found in the LICENSE file in the root directory).
*/

package jobcontroller

const (
	sqlTemplateCreateBatchTable = `CREATE TABLE IF NOT EXISTS %s
	(
		id                              bigint unsigned  NOT NULL AUTO_INCREMENT,
		batch_id                              varchar(256) NOT NULL, 
		batch_status							varchar(64)     NOT NULL DEFAULT 'queued',
    	count_size_when_creating_batch 						bigint unsigned  NOT NULL,
    	actually_affected_rows			bigint unsigned  NOT NULL DEFAULT 0,
    	batch_begin                     text        NOT NULL,
    	batch_end                       text        NOT NULL,
   		batch_sql                       text     NOT NULL,
    	batch_count_sql_when_creating_batch                       text     NOT NULL,
		PRIMARY KEY (id)
	) ENGINE = InnoDB`
)

const (
	sqlDMLJobGetJobsToSchedule = `select * from mysql.non_transactional_dml_jobs where status IN ('queued','not-in-time-period') order by id`
	sqlDMLJobGetAllJobs        = `select * from mysql.non_transactional_dml_jobs order by id`
	sqlDMLJobSubmit            = `insert into mysql.non_transactional_dml_jobs (
                                      job_uuid,
                                      dml_sql,
                                      table_schema,
                                      table_name,
                                      batch_info_table_schema,
                                      batch_info_table_name,
                                      status,
                                      status_set_time,
									  time_zone,
                                      fail_policy,
                                      running_time_period_start,
                                      running_time_period_end,
									  running_time_period_time_zone,
                                      batch_interval_in_ms,
                                      batch_size,
                                      throttle_expire_time,
                                      throttle_ratio,
                                      postpone_launch) values(%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a,%a)`

	sqlDMLJobUpdateMessage = `update mysql.non_transactional_dml_jobs set 
                                    message = %a 
                                where 
                                    job_uuid = %a`

	sqlDMLJobUpdateAffectedRows = `update mysql.non_transactional_dml_jobs set 
                                    affected_rows = affected_rows + %a 
                                where 
                                    job_uuid = %a`

	sqlDMLJobUpdateStatus = `update mysql.non_transactional_dml_jobs set 
                                    status = %a,
                                    status_set_time = %a
                                where 
                                    job_uuid = %a`

	sqlDMLJobGetInfo = `select * from mysql.non_transactional_dml_jobs 
                                where
                                	job_uuid = %a`

	sqlGetTablePk = ` show index from %s where key_name = 'primary'`

	sqlGetTableColNames = `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
								WHERE 
								    TABLE_SCHEMA = %a
									AND TABLE_NAME = %a`

	sqlDMLJobUpdateThrottleInfo = `update mysql.non_transactional_dml_jobs set 
                                    throttle_ratio = %a ,
                                    throttle_expire_time = %a
                                where 
                                    job_uuid = %a`

	sqlDMLJobClearThrottleInfo = `update mysql.non_transactional_dml_jobs set 
                                    throttle_ratio = NULL ,
                                    throttle_expire_time = NULL
                                where 
                                    job_uuid = %a`

	sqlDMLJobDeleteJob = `delete from mysql.non_transactional_dml_jobs where job_uuid = %a`

	sqlDMLJobUpdateTimePeriod = `update mysql.non_transactional_dml_jobs set 
                                    running_time_period_start = %a, 
                                    running_time_period_end = %a,
									running_time_period_time_zone = %a
                                where 
                                    job_uuid = %a`

	sqlGetIndexCount = `show index from %s`

	sqlGetDealingBatchID = `select dealing_batch_id from mysql.non_transactional_dml_jobs where job_uuid = %a`

	sqlUpdateDealingBatchID = `update mysql.non_transactional_dml_jobs set dealing_batch_id = %a where job_uuid = %a`

	sqlTemplateGetBatchSQLsByID = `select batch_sql,batch_count_sql_when_creating_batch from %s where batch_id = %%a`

	sqlTemplateGetMaxBatchID = `select batch_id as max_batch_id from %s order by id desc limit 1`

	sqlTempalteUpdateBatchStatusAndAffectedRows = `update %s set batch_status = %%a,actually_affected_rows = actually_affected_rows+%%a where batch_id = %%a`

	sqlTemplateUpdateBatchSQL = `update %s set batch_sql=%%a,batch_begin=%%a,batch_end=%%a where batch_id=%%a`

	sqlTemplateSelectPKCols = `select %s from %s.%s limit 1`

	sqlTemplateDropTable = `drop table if exiss %s`

	sqlTemplateGetBatchBeginAndEnd = `select batch_begin,batch_end from %s where batch_id=%%a`

	sqlTemplateGetBatchIDToExec = `SELECT batch_id FROM %s where batch_status = 'queued' order by CAST(SUBSTRING_INDEX(batch_id, '-', 1) AS SIGNED),id limit 1`

	sqlTemplateGetBatchStatus = `SELECT batch_status FROM %s where batch_id=%%a`

	sqlTemplateInsertBatchEntry = ` insert into %s (
		batch_id,
		batch_sql,
	 	batch_count_sql_when_creating_batch,
		count_size_when_creating_batch,
	 	batch_begin,
	 	batch_end
	) values (%%a,%%a,%%a,%%a,%%a,%%a)`

	sqlTemplateDropBatchTable = `drop table %s`

	sqlShowTablesLike = "SHOW TABLES LIKE '%a'"
)
