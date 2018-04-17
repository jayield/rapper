begin tran
	declare @accountId BIGINT
	declare @version rowversion
	exec dbo.AddUser 'maria@gmail.com', 2, '123', 'Maria', 'Finalista do curso', 'someurl', @accountId out, @version out
	select @accountId ID, @version [Version]
rollback

begin tran
	insert into Person (nif, name)
	OUTPUT CAST(INSERTED.version as bigint) version
	values ('123', 'Ze')
rollback

begin tran
	insert into Person (nif, name)
	values ('123', '456')

	select * from Person

	update Person set nif = '321'
	output CAST(INSERTED.version as bigint) version
	where nif = '123'
rollback

begin tran
	declare @version bigint
	exec dbo.UpdateUser 8, '432@hotmail.com', 4.0, '43532', 'Baril', 'O Baril sou eu', 'yetAnotherURL', @version out
	select @version [version]
rollback

/** Utils **/
select * from Account
select * from User
select * from Local
delete from ApiDatabase.Account


select P2.nif, P2.name, P2.birthday, CAST(P2.version as bigint) P2version, P1.studentNumber, CAST(P1.version as bigint) P1version, C.topGrade, C.year, CAST(C.version as bigint) version
from TopStudent C
  inner join Student P1 on C.nif = P1.nif
  inner join Person P2 on P1.nif = P2.nif

select P1.studentNumber, CAST(P1.version as bigint) P1version, P2.name, P2.birthday, CAST(P2.version as bigint) P2version, P2.nif, C.topGrade, C.year,
  CAST(C.version as bigint) Cversion from TopStudent C inner join Student P1 on C.nif = P1.nif inner join Person P2 on P1.nif = P2.nif where C.nif = 454
