CREATE TABLE IF NOT EXISTS mysql.branch
(
    `id`                  bigint unsigned  NOT NULL AUTO_INCREMENT,
    `name`                varchar(64)      NOT NULL,
    `source_host`         varchar(32)      NOT NULL,
    `source_port`         int              NOT NULL,
    `source_user`         varchar(64),
    `source_password`     varchar(64),
    `include`             varchar(256)     NOT NULL,
    `exclude`             varchar(256),
    `status`              varchar(32)      NOT NULL,
    `target_db_pattern`   varchar(256),
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
    ) ENGINE = InnoDB;
