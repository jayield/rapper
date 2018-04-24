USE PS_TEST_API_DATABASE
GO

if OBJECT_ID('populateDB') is not null
  drop proc populateDB
CREATE PROCEDURE populateDB
AS
  insert into Person (nif, name, birthday) values (321, 'Jose', '1996-6-2')
  insert into Person (nif, name, birthday) values (454, 'Nuno', '1996-4-2')

  insert into Car (owner, plate, brand, model) values (2, '23we45', 'Mitsubishi', 'lancer')

  insert into Student (nif, studentNumber) values (454, 3)

  insert into TopStudent (nif, topGrade, year) values (454, 20, 2017)

  insert into Company (id, cid, motto) values (1, 1, 'Living la vida loca')

  insert into Employee (name, companyId, companyCid) VALUES ('Bob', 1, 1)
  insert into Employee (name, companyId, companyCid) VALUES ('Charles', 1, 1)

  declare @empId1 bigint, @empId2 bigint

  select @empId1 = id from Employee where name = 'Bob'
  select @empId2 = id from Employee where name = 'Charles'

  insert into EmployeeJunior (id, juniorsYears) values (@empId1, 4)
  insert into EmployeeJunior (id, juniorsYears) values (@empId2, 1)
GO

select id, name, companyId, companyCid from Employee

if OBJECT_ID('deleteDB') is not null
  drop proc deleteDB
create procedure deleteDB
as
  delete from EmployeeJunior
  delete from CompanyEmployee
  delete from Employee
  delete from TopStudent
  delete from Student
  delete from Car
  delete from Person
  delete from Company
go