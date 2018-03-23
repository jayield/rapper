# Rapper
### ["I'm beginning to feel like a Rap God"](https://www.youtube.com/watch?v=XbGs_qK2PQA) 
DataMapper in rap for domain model

### Rapper Structure
 - There will be one Work Unit for each request received in the Web API. So each request will know what has been changed and what needs to be written to the BD.
 
 - Connection pool to establish links with the DB. This allows the re-use of connections already made to the DB, avoiding the creation of multiple connections.
 
 - Each entity mapper will have an Identity Map, which holds the recents objects read/altered from the DB.
 
 - The objects in the Identity Map are immutable, to change the data, a new immutable object will be created to be on the map. If writing in the DB is successful, the object will be placed on the map.
 
 - The Identity Map implements a LRU cache. They have a limit of what they can hold in memory, and when it reaches the limit, they will delete the least-read elements.
 
 - All domain objects must implement interface <code>DomainObject</code> and each domain object must have a field called version and the same for the DB tables.
 
 - The domain object classes must have the same name as respective DB table, same for the fields/rows.
 
 - The field corresponding to the primary key must have the annotation <code>@Id</code>. If it is a composed key it is needed to create a class that contains the keys and respective names, the domain object must have field of that class and mark it with the annotation <code>@EmbeddedId</code>.
 
### Rules
You must create an environment variable to connect to the DB. <br />
The environment variable must have the following format:

Name:

```sh
DB_CONNECTION_STRING
```

Value:

```sh
servername;database;user;password
```
