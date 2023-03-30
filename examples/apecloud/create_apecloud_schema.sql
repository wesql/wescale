create table read_write_splitting_policy_definition(
  sku varbinary(128),
  description varbinary(128),
  price bigint,
  primary key(sku)
) ENGINE=InnoDB;