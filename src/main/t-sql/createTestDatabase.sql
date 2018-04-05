IF DB_ID ('PS_TEST_API_DATABASE') IS NULL
	CREATE DATABASE PS_TEST_API_DATABASE;
GO

use PS_TEST_API_DATABASE

if EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'Person' AND Type = N'U')
    drop table Person
go
create table Person (
	nif int primary key,
	[name] nvarchar(50),
	birthday date,
  version rowversion
)
go