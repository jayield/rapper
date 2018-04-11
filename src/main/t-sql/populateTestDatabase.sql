USE PS_TEST_API_DATABASE
GO

/*EXEC dbo.AddAccount 'Test123@hotmail.com', 0.1, 'dgwuydguaw', null, null

exec dbo.AddUser '123@gmail.com', 2.0, '345', 'Hilário', 'Bue baril', 'someurl', null*/

insert into Person (nif, name, birthday) values (321, 'Jose', '1996-6-2')
insert into Person (nif, name, birthday) values (454, 'Nuno', '1996-4-2')

insert into Car (owner, plate, brand, model) values (2, '23we45', 'Mitsubishi', 'lancer')

insert into Student (nif, studentNumber) values (454, 3)

insert into TopStudent (nif, topGrade, year) values (454, 20, 2017)

insert into Company (id, cid, motto) values (1, 1, 'Living la vida loca')

insert into Employee (name, companyId, companyCid) VALUES ('Bob', 1, 1)
insert into Employee (name, companyId, companyCid) VALUES ('Charles', 1, 1)