USE PS_TEST_API_DATABASE
GO

/*EXEC dbo.AddAccount 'Test123@hotmail.com', 0.1, 'dgwuydguaw', null, null

exec dbo.AddUser '123@gmail.com', 2.0, '345', 'Hilário', 'Bue baril', 'someurl', null*/

insert into Person (nif, name, birthday) values (321, 'Jose', '1996-6-2')

insert into Car (owner, plate, brand, model) values (2, '23we45', 'Mitsubishi', 'lancer')