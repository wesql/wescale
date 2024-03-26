CREATE TABLE IF NOT EXISTS mysql.wescale_plugin
(
    `id`                     bigint unsigned NOT NULL AUTO_INCREMENT,
    `create_timestamp`       timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_timestamp`       timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    `name`                   varchar(256) NOT NULL,
    `description`            text,
    `priority`               int NOT NULL DEFAULT 1000,
    `status`                 varchar(64) NOT NULL DEFAULT 'ACTIVE' comment 'ACTIVE, INACTIVE, DRY_RUN',

    `plans`                  text,
    `table_names`            text,
    `query_regex`            text,

    `request_ip_regex`       varchar(64),
    `user_regex`             varchar(64),
    `leading_comment_regex`  text,
    `trailing_comment_regex` text,
    `bind_var_conds`         text,

    `action`                 varchar(64) NOT NULL comment 'CONTINUE, FAIL',
    `action_args`            text,
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`)
) ENGINE = InnoDB;