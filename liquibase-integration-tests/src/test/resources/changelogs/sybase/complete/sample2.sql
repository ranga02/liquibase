--liquibase formatted sql

--changeset auth1:1  runAlways:true
use srx
go

if exists (select 1 from sysobjects where type = 'U' and name = 'test1')
begin
    drop table test1
end
go
if exists (select 1 from sysobjects where type = 'U' and name = 'xyz')
begin
    drop table xyz
end



--changeset auth1:2 runAlways:true
select db_name()
go
create table xyz (
    id int primary key,
    name varchar(255)
)
go
create table test1 (
    id int primary key,
    name varchar(255)
)
go
insert xyz values(5, "hi")
go

use srx
go
--set quoted_identifier off
go
if exists (select 1 from sysobjects where type = 'U' and name = 'xyz2')
begin
    drop table xyz2
end
go
--changeset auth1:3 runAlways:true
create table xyz2 (
    id int primary key,
    name varchar(255)
)
go

--changeset auth1:4 runAlways:true

use unity
go

if exists (select 1 from sysobjects where type = 'U' and name = 'a_u')
begin
    drop table a_u
end
go

create table a_u (
    id int primary key,
    name varchar(255)
)
go
select * from unity..a_u
go


--changeset auth1:5 runAlways:true
use srx
go

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

--changeset auth1:6 runAlways:true
update test1 set name = "name3"
where id = 1

--changeset auth1:7 runAlways:true
select * from test1
go

set nocount on

print "HI"
PRINT "THERE"
go
