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

CREATE TABLE IF NOT EXISTS mysql.non_transactional_dml_jobs
(
    `id`                              bigint unsigned  NOT NULL AUTO_INCREMENT,
    `job_uuid`                  varchar(64)      NOT NULL UNIQUE,
    `table_schema`                     varchar(256)     NOT NULL,
    `table_name`                     varchar(256)     NOT NULL,
    `batch_info_table_schema`                  varchar(256)      NOT NULL,
    `batch_info_table_name`                  varchar(256)      NOT NULL UNIQUE,
    `postpone_launch`       tinyint unsigned NOT NULL DEFAULT '0',
    `status`                varchar(128)     NOT NULL,
    `status_set_time`           timestamp   NOT NULL,
    `time_zone`                 varchar(16)     NOT NULL,
    `message`                   varchar(2048)     NULL   DEFAULT NULL,
    `dml_sql`                       text    NOT NULL,
    `batch_interval_in_ms`             bigint        NOT NULL     ,
    `batch_size`              bigint        NOT NULL     ,
    `fail_policy`             varchar(64)     NOT NULL,
    `batch_concurrency`         bigint      NOT NULL DEFAULT 1     ,
    `throttle_ratio`        double NULL DEFAULT NULL,
    `throttle_expire_time` varchar(256)     NULL   DEFAULT NULL,
    `running_time_period_start` varchar(64)     NULL   DEFAULT NULL,
    `running_time_period_end`   varchar(64)    NULL   DEFAULT NULL,
    `running_time_period_time_zone`                 varchar(16)     NULL DEFAULT NULL,
    PRIMARY KEY (`id`),
    KEY `job_uuid_idx` (`job_uuid`),
    KEY `status_idx` (`status`)
) ENGINE = InnoDB;
