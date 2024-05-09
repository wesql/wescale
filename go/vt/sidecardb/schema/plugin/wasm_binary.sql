CREATE TABLE IF NOT EXISTS mysql.wasm_binary
(
    `id`                              bigint unsigned NOT NULL AUTO_INCREMENT,
    `create_timestamp`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_timestamp`                timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `name`                            varchar(256) NOT NULL,
    `runtime`                         text NOT NULL,
    `data`                             MEDIUMTEXT NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
    ) ENGINE = InnoDB;