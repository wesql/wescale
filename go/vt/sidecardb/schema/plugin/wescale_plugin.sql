CREATE TABLE IF NOT EXISTS mysql.wescale_plugin
(
    `id`                             bigint unsigned NOT NULL AUTO_INCREMENT,
    `create_timestamp`               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_timestamp`               timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    `name`                           varchar(256) NOT NULL,
    `description`                    text,
    `priority`                       int NOT NULL,

    `plans`                          text,
    `tableNames`                     varchar(1024),
    `sql_regex`                      text,

    `request_ip_regex`               varchar(64),
    `user_regex`                     varchar(64),
    `leadingComment_regex`           text,
    `trailingComment_regex`          text,
    `bindVarConds`                   text,

    `action`                         varchar(64) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
) ENGINE = InnoDB;