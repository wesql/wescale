CREATE TABLE IF NOT EXISTS mysql.branch_snapshots
(
    `id`                             bigint unsigned  NOT NULL AUTO_INCREMENT,
    `create_timestamp`               timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_timestamp`               timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `workflow_name`                  varchar(64)      NOT NULL,
    `schema_snapshot`                longblob,
    PRIMARY KEY (`id`),
    UNIQUE KEY(`workflow_name`)
) ENGINE = InnoDB;
