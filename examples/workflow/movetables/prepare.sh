#!/bin/bash
# Copyright ApeCloud, Inc.
# Licensed under the Apache v2(found in the LICENSE file in the root directory).
source "$(dirname "${BASH_SOURCE[0]:-$0}")/../../common/env.sh"


mysql -h127.0.0.1 -P15306 -e 'create database if not exists movetables_source'
mysql -h127.0.0.1 -P15306 -e 'create database if not exists movetables_target'

mysql -h127.0.0.1 -P15306 -e 'create table if not exists movetables_source.product(
                                sku varbinary(128),
                                description varbinary(128),
                                price bigint,
                                primary key(sku)
                              ) ENGINE=InnoDB;
                              create table if not exists movetables_source.customer(
                                customer_id bigint not null auto_increment,
                                email varbinary(128),
                                primary key(customer_id)
                              ) ENGINE=InnoDB;
                              create table if not exists movetables_source.corder(
                                order_id bigint not null auto_increment,
                                customer_id bigint,
                                sku varbinary(128),
                                price bigint,
                                primary key(order_id)
                              ) ENGINE=InnoDB;'
sleep 1

