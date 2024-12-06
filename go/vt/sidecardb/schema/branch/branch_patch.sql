CREATE TABLE IF NOT EXISTS mysql.branch_patch
(
    `id`               bigint unsigned NOT NULL AUTO_INCREMENT,
    `name`             varchar(64)     NOT NULL,
    `database`         varchar(64)     NOT NULL,
    `table`           varchar(64)     NOT NULL,
    `ddl` text           NOT NULL,
    `merged`          bool NOT NULL,
    PRIMARY KEY (`id`),
--     one table will have multiple DDLs
    UNIQUE KEY(`id`, `name`, `database`, `table`)
    ) ENGINE = InnoDB;