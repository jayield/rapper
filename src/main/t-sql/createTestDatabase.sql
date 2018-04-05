IF DB_ID ('PS_TEST_API_DATABASE') IS NULL
	CREATE DATABASE PS_TEST_API_DATABASE;
GO

use PS_TEST_API_DATABASE

if object_id('Person') is not null
    drop table Person
go
create table Person (
	nif int primary key,
	[name] nvarchar(50),
	birthday date
)
go