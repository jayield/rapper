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

if EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'Car' AND Type = N'U')
  drop table Car
go
create table Car (
  owner int,
  plate varchar(6),
  brand varchar(20),
  model varchar(20),
  version rowversion,

  PRIMARY KEY (owner, plate)
)
go

if EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'Student' AND Type = N'U')
  drop table Student
go
create table Student (
  nif int references Person,
  studentNumber int,
  version rowversion,

  PRIMARY KEY (nif)
)
go

if EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'TopStudent' AND Type = N'U')
  drop table TopStudent
go
create table TopStudent (
  nif int references Student,
  topGrade int,
  year int,
  version rowversion,

  PRIMARY KEY (nif)
)
go

if EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'Company' AND Type = N'U')
  drop table Company
go
create table Company (
  id int,
  cid int,
  motto varchar(20),
  version rowversion,

  PRIMARY KEY (id, cid)
)
go

if EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'Employee' AND Type = N'U')
  drop table Employee
go
create table Employee (
  id int identity,
  name varchar(20),
  version rowversion,

  PRIMARY KEY (id)
)
go

if EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'CompanyEmployee' AND Type = N'U')
  drop table CompanyEmployee
go
create table CompanyEmployee (
  employeeId int references Employee,
  companyId int,
  companyCid int,
  version rowversion,

  PRIMARY KEY (employeeId, companyId, companyCid),
  FOREIGN KEY (companyId, companyCid) REFERENCES Company (id, cid)
)
go
