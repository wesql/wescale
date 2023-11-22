CREATE TABLE IF NOT EXISTS mysql.branch_table_rules
(
    `id`                                bigint unsigned  NOT NULL AUTO_INCREMENT,
    `workflow_name`                     varchar(64)      NOT NULL,
    `source_table_name`                 varchar(128)     NOT NULL,
    `target_table_name`                 varchar(128)     NOT NULL,
    `filtering_rule`                    text,
    `create_ddl`                        text,
    `merge_ddl`                         text,
    PRIMARY KEY (`id`),
    KEY (`workflow_name`)
) ENGINE = InnoDB;
