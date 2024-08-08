CREATE TABLE IF NOT EXISTS mysql.cdc_consumer
(
    `id`                              bigint unsigned NOT NULL AUTO_INCREMENT,
    `create_timestamp`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_timestamp`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `enable`                          tinyint(1) NOT NULL DEFAULT 1,
    `name`                            varchar(256) NOT NULL,
    `wasm_binary_name`                varchar(256) NOT NULL,
    `tag`                             varchar(256) NOT NULL COMMENT 'vector,redis',
    `env`                             TEXT DEFAULT NULL,
--     todo begin_position/pk, current_position/pk, end_position/pk, status
    last_gtid                         TEXT DEFAULT NULL,
    last_pk                           varbinary(2000) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
) ENGINE = InnoDB;