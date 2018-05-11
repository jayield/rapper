
create table Person (
  nif int primary key,
  name varchar(50),
  birthday date,
  version bigint default 1
);

create table Car (
  owner int,
  plate varchar(6),
  brand varchar(20),
  model varchar(20),
  version bigint default 1,

  PRIMARY KEY (owner, plate)
);

create table Student (
  nif int references Person,
  studentNumber int,
  version bigint default 1,

  PRIMARY KEY (nif)
);

create table TopStudent (
  nif int references Student,
  topGrade int,
  year int,
  version bigint default 1,

  PRIMARY KEY (nif)
);

create table Company (
  id int,
  cid int,
  motto varchar(20),
  version bigint default 1,

  PRIMARY KEY (id, cid)
);

create table Employee (
  id int identity,
  name varchar(20),
  companyId int,
  companyCid int,
  version bigint default 1,

  FOREIGN KEY (companyId, companyCid) references Company (id, cid)
);

create table Book (
  id bigint identity,
  name varchar(20),
  version bigint default 1
);

create table Author (
  id bigint identity,
  name varchar(20),
  version bigint default 1
);

create table BookAuthor (
  bookId bigint references Book,
  authorId bigint references Author,
  version bigint default 1,

  PRIMARY KEY (bookId, authorId)
);

create table Dog (
  name varchar(40),
  race varchar(20),
  age int,

  PRIMARY KEY (name, race)
);

CREATE PROCEDURE populateDB()
MODIFIES SQL DATA
begin atomic
  declare authorId int;
  declare bookId int;
begin atomic
  insert into Person (nif, name, birthday) values (321, 'Jose', '1996-6-2');
  insert into Person (nif, name, birthday) values (454, 'Nuno', '1996-4-2');

  insert into Car (owner, plate, brand, model) values (2, '23we45', 'Mitsubishi', 'lancer');

  insert into Student (nif, studentNumber) values (454, 3);
  insert into TopStudent (nif, topGrade, year) values (454, 20, 2017);

  insert into Company (id, cid, motto) values (1, 1, 'Living la vida loca');
  insert into Company (id, cid, motto) values (1, 2, 'Living vida loca');

  insert into Employee (name, companyId, companyCid) VALUES ('Bob', 1, 1);
  insert into Employee (name, companyId, companyCid) VALUES ('Charles', 1, 1);

  insert into Author(name) values ('Ze');
  insert into Book(name) values ('1001 noites');

  set authorId = (select id from Author where name = 'Ze');
  set bookid = (select id from Book where name = '1001 noites');

  insert into BookAuthor(bookId, authorId) values (bookId, authorId);
  insert into Dog(name, race, age) values ('Doggy', 'Bulldog', 5);
end;
end;

create procedure deleteDB()
MODIFIES SQL DATA
begin atomic
  delete from BookAuthor;
  delete from Author;
  delete from Book;
  delete from Employee;
  delete from TopStudent;
  delete from Student;
  delete from Car;
  delete from Person;
  delete from Company;
  delete from Dog;
end;

CREATE TRIGGER person_trigger
  before update ON Person REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
        set NEW.version = old.version +1;
    END;
  END;

CREATE TRIGGER car_trigger
  before update ON Car REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;

CREATE TRIGGER student_trigger
  before update ON Student REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;

CREATE TRIGGER TopStudent_trigger
  before update ON TopStudent REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;

CREATE TRIGGER Company_trigger
  before update ON Company REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;

CREATE TRIGGER Employee_trigger
  before update ON Employee REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;

CREATE TRIGGER Book_trigger
  before update ON Book REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;

CREATE TRIGGER Author_trigger
  before update ON Author REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;

CREATE TRIGGER BookAuthor_trigger
  before update ON BookAuthor REFERENCING NEW ROW AS NEW OLD AS OLD
  FOR EACH ROW
  BEGIN ATOMIC
    BEGIN ATOMIC
      set NEW.version = OLD.version +1;
    END;
  END;
