--liquibase formatted sql



--changeset nvoxland:21 runAlways:true
use srx
go

if exists (select 1 from sysobjects where type = 'U' and name = 'test1')
begin
    drop table test1
end
go

--changeset nvoxland:2 runAlways:true
use unity
go

select db_name()
go
use srx
go
--set quoted_identifier off
go
--changeset nvoxland:3 runAlways:false
create table xyz (
    id xint primary key,
    name varchar(255)
)
go

--changeset nvoxland:31 runAlways:false
create table test1 (
    id int primary key,
    name varchar(255)
)
go

--rollback drop table test1;

--changeset nvoxland:4 runAlways:true

insert into test1 (id, name) values (1, 'name 1')
insert into test1 (id, name) values (2, 'name 2')
select * from test1
print 'HELLO'
select  "HI THERE3"

select p = "something good"
select @@error, @@rowcount, @@transtate
set nocount on
exec sp_who 'rnandakumar'
set nocount off
select db_name(), @@servername
--exec srx..x_p1

select "HELLO"
go

--changeset nvoxland:5 runAlways:true
update test1 set name = "name3"
where id = 1

--changeset nvoxland:6 runAlways:true
select * from test1
go

