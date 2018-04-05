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
select * from [Account]
select * from [User]
select * from [Local]
delete from ApiDatabase.Account
