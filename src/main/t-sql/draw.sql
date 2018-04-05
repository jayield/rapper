begin tran
	declare @accountId BIGINT
	declare @version rowversion
	exec dbo.AddUser 'maria@gmail.com', 2, '123', 'Maria', 'Finalista do curso', 'someurl', @accountId out, @version out
	select @accountId ID, @version [Version]
rollback

begin tran
	insert into [Local] ([Address], Country, Street, ZIPCode)
	OUTPUT INSERTED.[version]
	values ('123', '456', '789', '101112')
rollback

begin tran
	insert into [Local] ([Address], Country, Street, ZIPCode)
	values ('123', '456', '789', '101112')

	select * from [Local]

	update [Local] set country = '321' 
	output INSERTED.[version]
	where [Address] = '123'
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
