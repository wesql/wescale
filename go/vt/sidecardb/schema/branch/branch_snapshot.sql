CREATE TABLE IF NOT EXISTS mysql.branch_snapshot
(
    `id`               bigint unsigned NOT NULL AUTO_INCREMENT,
    `name`             varchar(64)     NOT NULL,
    `database`         varchar(64)     NOT NULL,
    `table`           varchar(64)     NOT NULL,
    `create_table_sql` text           NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY(`name`, `database`, `table`)
    ) ENGINE = InnoDB;