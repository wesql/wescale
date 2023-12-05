CREATE TABLE IF NOT EXISTS mysql.branch_table_rules
(
    `id`                                bigint unsigned  NOT NULL AUTO_INCREMENT,
    `workflow_name`                     varchar(64)      NOT NULL,
    `source_table_name`                 varchar(128)     NOT NULL,
    `target_table_name`                 varchar(128)     NOT NULL,
    `filtering_rule`                    text,
    `create_ddl`                        text,
    `merge_ddl`                         text,
    `skip_copy_phase`                   tinyint unsigned NOT NULL DEFAULT '1',
    `need_merge_back`                   tinyint unsigned NOT NULL DEFAULT '1',
    `merge_ddl_uuid`                    varchar(128),
    `default_filter_rules`              text,
    PRIMARY KEY (`id`),
    KEY (`workflow_name`)
) ENGINE = InnoDB;
