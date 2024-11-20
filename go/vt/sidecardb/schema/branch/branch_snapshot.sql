CREATE TABLE IF NOT EXISTS mysql.branch_snapshots
(
    `id`       bigint unsigned NOT NULL AUTO_INCREMENT,
    `name`     varchar(64)     NOT NULL,
    `snapshot` longblob        NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
    ) ENGINE = InnoDB;
