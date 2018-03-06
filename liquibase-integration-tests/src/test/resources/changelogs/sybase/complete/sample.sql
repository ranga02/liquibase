--liquibase formatted sql

--changeset nvoxland:1 runAlways:true
use srx
go

if exists (select 1 from sysobjects where type = 'U' and name = 'test1')
begin
    drop table test1
end
go

--changeset nvoxland:2 runAlways:false
select 4
go

--changeset nvoxland:3 runAlways:false
create table test1 (
    id int primary key,
    name varchar(255)
)
go

--changeset nvoxland:4 runAlways:false
select 'rnandakumar'
print "wow"
go

--changeset nvoxland:5
--use release_maint
--go

print "wow"
go
