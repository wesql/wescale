CREATE TABLE IF NOT EXISTS mysql.branch
(
    `id`                            bigint unsigned  NOT NULL AUTO_INCREMENT,
    `name`                          varchar(64)      NOT NULL,
    `source_host`                   varchar(32)      NOT NULL,
    `source_port`                   int              NOT NULL,
    `source_user`                   varchar(64),
    `source_password`               varchar(64),
    `include_databases`             varchar(256)     NOT NULL,
    `exclude_databases`             varchar(256),
    `status`                        varchar(32)      NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
    ) ENGINE = InnoDB;
