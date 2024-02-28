CREATE TABLE IF NOT EXISTS mysql.branch_jobs
(
    `id`                             bigint unsigned  NOT NULL AUTO_INCREMENT,
    `create_timestamp`               timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_timestamp`               timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `source_database`                varchar(256)     NOT NULL,
    `target_database`                varchar(256)     NOT NULL,
    `external_cluster`               varchar(256)     NOT NULL,
    `workflow_name`                  varchar(64)      NOT NULL,
    `source_topo`                    varchar(256)     NOT NULL,
    `source_tablet_type`             varchar(64)      NOT NULL,
    `cells`                          varchar(64)      NOT NULL,
    `stop_after_copy`                tinyint unsigned NOT NULL DEFAULT '1',
    `onddl`                          varchar(256),
    `status`                         varchar(64)      NOT NULL,
    `message`                        varchar(256),
    `merge_timestamp`                timestamp        DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY(`workflow_name`)
    ) ENGINE = InnoDB;
