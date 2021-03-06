# Rapper

[![Build Status](https://sonarcloud.io/api/project_badges/measure?project=com.github.jayield%3Arapper&metric=alert_status)](https://sonarcloud.io/dashboard?id=com.github.jayield%3Arapper)
[![Maven Central Version](http://img.shields.io/maven-central/v/com.github.jayield/rapper.svg)](http://search.maven.org/#search%7Cga%7C1%7Cjayield%20rapper)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=com.github.jayield%3Arapper&metric=coverage)](https://sonarcloud.io/component_measures?id=com.github.jayield%3Arapper&metric=Coverage)

### ["I'm beginning to feel like a Rap God"](https://www.youtube.com/watch?v=XbGs_qK2PQA) 
DataMapper in rap for domain model

Rapper is a Reactive Data Mapper library for Java, which provides an implementation of a Data Mapper for a domain entity. 
This implementations follows the principles stated by Martin Fowler in [[Fowler, 2002]][#Fowler2002] about Object-Relational Behavioral 
and Data Source Architectural Patterns, but applied to a reactive approach. Thus the Rapper API is asynchronous and the data mapper effects 
are strongly typed in Promises [[Friedman and Wise, 1976]][#FriedmanAndWise1976].

The Rapper implementation use the following design patterns: 

* [Unit of Work](https://martinfowler.com/eaaCatalog/unitOfWork.html)
* [Data Mapper](https://martinfowler.com/eaaCatalog/dataMapper.html)
* [Identity Map](https://martinfowler.com/eaaCatalog/identityMap.html)
* etc…

[#Fowler2002]: https://dl.acm.org/citation.cfm?id=579257 "Patterns of Enterprise Application Architecture"
[#FriedmanAndWise1976]: https://books.google.pt/books/about/The_Impact_of_Applicative_Programming_on.html?id=ZIhtHQAACAAJ  "The Impact of Applicative Programming on Multiprocessing"

### Structure
- `MapperRegistry` is the class to aim for when we want to manipulate the data. This static class supplies `DataMapper` instances for data manipulation.

- Each `UnitOfWork` will have an **Identity Map**, which holds the recents objects read/altered from the DB.

- The objects in the **Identity Map** are immutable, to change the data, a new immutable object will be created to replace the one in the map. The replacement 
 will only happen if the commit is successful.
 
- A `UnitOfWork` will have a transaction per commit, this means, when a commit is done on a `UnitOfWork`, a new transaction will be opened as soon a connection
to the DB is needed.
 
- There will be a **connection pool** to establish links with the DB. This allows the re-use of connections already made, avoiding the 
creation of multiple connections.

- The **Identity Map** implements a LRU cache. They have a limit of what they can hold in memory, and when it reaches the limit, 
they will delete the least-read elements.
 
- A `DomainObject` may contain `Function<UnitOfWork, CompletableFuture<DomainObject>>` as a reference to another table. The field holding it, must be annotated with 
<code>@ColumnName</code>, in which is passed `name` as the name of the column(s) in the representing table where the ID of the external `DomainObject` 
takes place.

- A `DomainObject` may also contain a `Function<UnitOfWork, CompletableFuture<List<DomainObject>>>` as a reference to another table. The field holding it, must be annotated with 
<code>@ColumnName</code>, in which is passed `foreignName` as the name of the column(s) in the referenced table where the ID of the `DomainObject` 
takes place, the `table` in case of N-N relation and the `externalName` which is the name of the column(s) in the referenced table 
where the ID of the external `DomainObject` takes place. `externalName` is only needed when `table` is given.

- Each `DomainObject` may have a version field (an auto incremented field on insertions and updates). This field must 
be annotated with `@Version` and it will be used to successful synchronise the in-memory data and DB as it will only allow writes in the DB, 
if the `DomainObject`'s `version` that is being written exists on it. 
 
### Rules
- You must create an environment variable to connect to the DB. The environment variable must have the following format:

Name:

```sh
DB_CONNECTION_STRING
```

Value:

```sh
jdbcURL%;user%;password
```

- All domain objects must implement interface <code>DomainObject</code>.

- The domain object classes must have the same name as respective DB table, same for the fields/rows.

- The field corresponding to the primary key must have the annotation <code>@Id</code> and if it's auto-generated by the DB, 
`isIdentity()` must return true. If it is a composed key it is needed to create a class that extends `EmbeddedIdClass`. This class must contain
the keys and respective names, the domain object must have a field of that class and mark it with the annotation <code>@EmbeddedId</code>.

- The class that extends `EmbeddedIdClass` must call its super on the contructor, passing the values of the ids

- A `DomainObject` and the field annotated with <code>@EmbeddedId</code> must have a 0 arguments constructor.

### Examples

An example illustrating the correct use of DomainObject interface and the annotations.

```java
public class Book implements DomainObject<Long> {

    @Id(isIdentity = true)
    private long id;
    private String name;
    private long version;

    @ColumnName(foreignName = "bookId", table = "BookAuthor", externalName = "authorId")
    private Function<UnitOfWork, CompletableFuture<List<Author>>> authors;

    public Book(long id, String name, long version, Function<UnitOfWork, CompletableFuture<List<Author>>> authors) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.authors = authors;
    }

    public Book() { }

    public String getName() {
        return name;
    }

    public Function<UnitOfWork, CompletableFuture<List<Author>>> getAuthors() {
        return authors;
    }

    @Override
    public Long getIdentityKey() {
        return id;
    }

    @Override
    public long getVersion() {
        return version;
    }
}
```

The table `Book` has an N-N relation with table `Author`, being `BookAuthor` the table that holds both table's primary keys.
This table has 2 columns, being `bookId` the id of the `Book` and `authorId` the id of the `Author`. As such `Book` class 
has a `CompletableFuture<List<Author>>` with the references to the book's 
authors. \
The primary key of the table `Book` is an auto incremented value, so we mark it with the annotation `@Id` and say it's an identity value

#### Disclaimers
- Although the API is asynchronous, JDBC driver is blocking and thus this internal implementation is blocking too.

- Because of the use of vertx-jdbc-client library, we do not support fields of type **short**