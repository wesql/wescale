/*
Copyright 2023 The Vitess Authors.

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

-- todo newborn22 ,需要索引？，名字
-- todo newbron22 ,改成batchsize
CREATE TABLE IF NOT EXISTS mysql.big_dml_jobs_table
(
    `id`                              bigint unsigned  NOT NULL AUTO_INCREMENT,
    `job_uuid`                  varchar(64)      NOT NULL UNIQUE,
    `job_batch_table`                  varchar(64)      NOT NULL UNIQUE,
    `job_status`                varchar(128)     NOT NULL,
    `status_set_time`           varchar(128)     NOT NULL,
    `message`                   varchar(2048)     NULL   DEFAULT NULL,
    `dml_sql`                       varchar(256)     NOT NULL,
    `related_schema`                     varchar(256)     NOT NULL,
    `related_table`                     varchar(256)     NOT NULL,
    `timegap_in_ms`             bigint        NOT NULL     ,
    `batch_size`              bigint        NOT NULL     ,
    `affected_rows`        bigint        NOT NULL     DEFAULT 0,
    `throttle_ratio`        double NULL DEFAULT NULL,
    `throttle_expire_time` varchar(256)     NULL   DEFAULT NULL,
    `dealing_batch_id`        varchar(256) NULL DEFAULT NULL,
    PRIMARY KEY (`id`)
) ENGINE = InnoDB
