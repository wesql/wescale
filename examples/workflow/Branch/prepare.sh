#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"

USER_COUNT=50
CUSTOMER_COUNT=50
PRODUCT_COUNT=50
CORDER_COUNT=200

mysql -h127.0.0.1 -P15307 -e 'create database if not exists branch_source'

mysql -h127.0.0.1 -P15307 -e 'create table if not exists branch_source.product(
                                sku varchar(128),
                                description varchar(128),
                                price bigint,
                                primary key(sku)
                              ) ENGINE=InnoDB;
                              create table if not exists branch_source.customer(
                                customer_id bigint not null auto_increment,
                                email varchar(128),
                                primary key(customer_id)
                              ) ENGINE=InnoDB;
                              create table if not exists branch_source.corder(
                                order_id bigint not null auto_increment,
                                customer_id bigint,
                                sku varchar(128),
                                price bigint,
                                primary key(order_id)
                              ) ENGINE=InnoDB;
                              CREATE TABLE if not exists branch_source.user (
                                  id INT AUTO_INCREMENT PRIMARY KEY auto_increment,
                                  name VARCHAR(255) NOT NULL
                              ) ENGINE=InnoDB;'

sleep 5

function insert_users() {
    local start=1
    local end=$1

    while ((start <= end))
    do
        local batch_end=$((start + 999))
        if ((batch_end > end)); then
            batch_end=$end
        fi

        SQL="INSERT INTO branch_source.user (id, name) VALUES "
        COMMA=""
        for i in $(seq $start $batch_end)
        do
            SQL="${SQL}${COMMA}($i, CONCAT('user', FLOOR(1 + RAND() * 999999)))"
            COMMA=", "
        done
        mysql -h127.0.0.1 -P15307 -e "$SQL"

        start=$((batch_end + 1))
    done
}

function insert_customer() {
    local start=1
    local end=$1

    while ((start <= end))
    do
        local batch_end=$((start + 999))
        if ((batch_end > end)); then
            batch_end=$end
        fi

        SQL="INSERT INTO branch_source.customer (customer_id, email) VALUES "
        COMMA=""
        for i in $(seq $start $batch_end)
        do
            SQL="${SQL}${COMMA}($i, CONCAT('user', FLOOR(1 + RAND() * 999999), '@domain.com'))"
            COMMA=", "
        done
        mysql -h127.0.0.1 -P15307 -e "$SQL"

        start=$((batch_end + 1))
    done
}

function insert_product() {
    local start=1
    local end=$1

    while ((start <= end))
    do
        local batch_end=$((start + 999))
        if ((batch_end > end)); then
            batch_end=$end
        fi

        SQL="INSERT INTO branch_source.product (sku, description, price) VALUES "
        COMMA=""
        for i in $(seq $start $batch_end)
        do
            SQL="${SQL}${COMMA}(CONCAT('SKU-', $i), 'product description', $i)"
            COMMA=", "
        done
        mysql -h127.0.0.1 -P15307 -e "$SQL"

        start=$((batch_end + 1))
    done
}

function insert_corder() {
    local start=1
    local end=$1

    while ((start <= end))
    do
        local batch_end=$((start + 999))
        if ((batch_end > end)); then
            batch_end=$end
        fi

        SQL="INSERT INTO branch_source.corder (order_id, customer_id, sku, price) VALUES "
        COMMA=""
        for i in $(seq $start $batch_end)
        do
            random_customer_id=$((1 + RANDOM % $CUSTOMER_COUNT))
            random_sku_id=$((1 + RANDOM % $PRODUCT_COUNT))
            SQL="${SQL}${COMMA}($i, $random_customer_id, CONCAT('SKU-', $random_sku_id), $random_sku_id)"
            COMMA=", "
        done
        mysql -h127.0.0.1 -P15307 -e "$SQL"

        start=$((batch_end + 1))
    done
}

insert_users $USER_COUNT
insert_customer $CUSTOMER_COUNT
insert_product $PRODUCT_COUNT
insert_corder $CORDER_COUNT