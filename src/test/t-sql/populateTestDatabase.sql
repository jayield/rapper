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
  insert into Company (id, cid, motto) values (1, 2, 'Living vida loca')

  insert into Employee (name, companyId, companyCid) VALUES ('Bob', 1, 1)
  insert into Employee (name, companyId, companyCid) VALUES ('Charles', 1, 1)

  declare @authorId int, @bookId int

  insert into Author(name) values ('Ze')
  insert into Book(name) values ('1001 noites')

  select @authorId = id from Author where name = 'Ze'
  select @bookId = id from Book where name = '1001 noites'

  insert into BookAuthor(bookId, authorId) values (@bookId, @authorId)

  insert into Dog(name, race, age) values ('Doggy', 'Bulldog', 5)
GO

if OBJECT_ID('deleteDB') is not null
  drop proc deleteDB
create procedure deleteDB
as
  delete from Author
  delete from Book
  delete from BookAuthor
  delete from Employee
  delete from TopStudent
  delete from Student
  delete from Car
  delete from Person
  delete from Company
go