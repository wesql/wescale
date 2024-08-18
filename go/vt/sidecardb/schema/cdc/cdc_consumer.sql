CREATE TABLE IF NOT EXISTS mysql.cdc_consumer
(
    `id`                              bigint unsigned NOT NULL AUTO_INCREMENT,
    `create_timestamp`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_timestamp`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `name`                            varchar(256) NOT NULL,
    `description`                     TEXT NOT NULL COMMENT 'vector,redis',
    `enable`                          tinyint(1) NOT NULL DEFAULT 1,
    `wasm_binary_name`                varchar(256) NOT NULL,
    `env`                             TEXT DEFAULT NULL,
--     todo begin_position/pk, current_position/pk, end_position/pk, status
    last_gtid                         TEXT DEFAULT NULL,
    last_pk                           varbinary(2000) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
) ENGINE = InnoDB;