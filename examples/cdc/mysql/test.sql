create table t1 (c1 int primary key auto_increment, c2 text);
create table t2 (c1 int primary key auto_increment, c2 text);
create table t2_meta (id bigint unsigned not null auto_increment, last_gtid varchar(128) DEFAULT NULL, last_pk varbinary(2000) DEFAULT NULL, lastpk_str varchar(512) DEFAULT NULL, primary key (id));

insert into t1 (c2) values ('I want you to act as a linux terminal. I will type commands and you will reply with what the terminal should show.');
insert into t1 (c2) values ('I want you to act as an English translator, spelling corrector and improver.');
insert into t1 (c2) values ('I want you to act as an interviewer.');
insert into t1 (c2) values ('I want you to act as an engineer.');

delete from t1 where c2 = 'I want you to act as an English translator, spelling corrector and improver.';

update t1 set c1 = 12345 where c2 = 'I want you to act as an interviewer.';