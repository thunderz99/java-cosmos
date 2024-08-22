# java-cosmos: A lightweight Azure CosmosDB client for Java

java-cosmos is a client for Azure CosmosDB 's SQL API (also called documentdb formerly). Which is an opinionated library aimed at ease of use for CRUD and find (aka. query).

[![Java CI with Maven](https://github.com/thunderz99/java-cosmos/actions/workflows/maven.yml/badge.svg)](https://github.com/thunderz99/java-cosmos/actions/workflows/maven.yml)

## Background

* Microsoft's official Java CosmosDB client is verbose to use

## Disclaimer

* This is an alpha version, and features are focused to CRUD and find at present.
* Mininum supported Java runtime: JDK 17

## Quickstart

### Add dependency

```xml
<!-- Add new dependency -->

<dependency>
  <groupId>com.github.thunderz99</groupId>
    <artifactId>java-cosmos</artifactId>
    <version>0.6.8</version>
</dependency>
```

### Start programming

```java
import io.github.thunderz99.cosmos.Cosmos

import java.util.ArrayList;

public static void main(String[]args){
        var db=new Cosmos(System.getenv("YOUR_CONNECTION_STRING")).getDatabase("Database1")
        db.upsert("Collection1",new User("id011","Tom","Banks"))

        var cond=Condition.filter(
        "id","id010", // id equal to 'id010'
        "lastName","Banks", // last name equal to Banks
        "firstName !=","Andy", // not equal
        )
        .sort("lastName","ASC") //optional order
        .offset(0) //optional offset
        .limit(100); //optional limit

        var users=db.find("Collection1",cond).toList(User.class)
        }

class User {

    public String id;
    public String firstName;
    public String lastName;
    public String location;
    public int age;
    public List<Parent> parents = new ArrayList<>();

    public User(String id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public User(String id, String firstName, String lastName,List<Parent> parents) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.parents=parents;
    }
}

class Parent{
    public String firstName;
    public String lastName;

    public Parent(String firstName, String lastName){
        this.firstName = firstName;
        this.lastName = lastName;
    }
}

```

## More examples

### Work with partitions

```java
// Save user into Coll:Collection1, partition:Users.
// If you do not specify the partition. It will default to coll name.
var db = new Cosmos(System.getenv("YOUR_CONNECTION_STRING")).getDatabase("Database1");
db.upsert("Collection1", new User("id011", "Tom", "Banks",List.of(new Parent("Tom","John"),new Parent("Jerry":"Jones"))), "Users");

// The default partition key is "_partition", so we'll get a json like this:
// {
//    "id": "id011",
//    "firstName": "Tom",
//    "lastName": "Banks",
//    "parents": [{"firstName": "Tom","lastName": "John"},{"firstName": "Tom","Jerry": "Jones"}],
//    "_partition": "Users"
// }
```

### Create database and collection dynamically

```java
var cosmos = new Cosmos(System.getenv("YOUR_CONNECTION_STRING"));
var db = cosmos.createIfNotExist("Database1", "Collection1");
db.upsert("Collection1", new User("id011", "Tom", "Banks"))

// create a collection with uniqueIndexPolicy
var uniqueKeyPolicy = Cosmos.getUniqueKeyPolicy(Set.of("/_uniqueKey1", "/_uniqueKey2"));
var db = cosmos.createIfNotExist("Database1", "Collection2", uniqueKeyPolicy);
```

### CRUD

```java
var db = new Cosmos(System.getenv("YOUR_CONNECTION_STRING")).getDatabase("Database1");

// Create
db.create("Collection1", new User("id011", "Tom", "Banks"), "Users");

// Read
var user1 = db.read("Collection1", "id001", "Users").toObject(User.class);

// Update
user1.lastName = "Updated";
db.update("Collection1", user1, "Users");

// Upsert
db.upsert("Collection1", user1, "Users");

// Delete
db.delete("Collection1", user1,id, "Users");
```

### Batch Operation
> Note: Batch operation is transactional. The maximum number of operations is 100.
> https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=java
```java
var db = new Cosmos(System.getenv("YOUR_CONNECTION_STRING")).getDatabase("Database1");

// Create
db.batchCreate("Collection1", List.of(new User("id011", "Tom", "Banks")), "Users");

// Upsert
db.batchUpsert("Collection1", List.of(user1), "Users");

// Delete
db.batchDelete("Collection1", List.of(user1), "Users");
// or
db.batchDelete("Collection1", List.of(user1.id), "Users");

```

### Bulk Operation
> Note: Bulk operation is NOT transactional. Have no number limit in theoretically.
> https://learn.microsoft.com/en-us/azure/cosmos-db/bulk-executor-overview
```java
var db = new Cosmos(System.getenv("YOUR_CONNECTION_STRING")).getDatabase("Database1");

// Create
db.bulkCreate("Collection1", List.of(new User("id011", "Tom", "Banks")), "Users");

// Upsert
db.bulkUpsert("Collection1", List.of(user1), "Users");

// Delete
db.bulkDelete("Collection1", List.of(user1), "Users");
// or
db.bulkDelete("Collection1", List.of(user1.id), "Users");

```

### Partial Update

```java
// Do a partial update. This is implemented by reading the original data / merging original data and the partial data to update / updating using the merged data
db.updatePartial("Collection", user1.id, Map.of("age", 20, "lastName", "Hanks",...), "Users")

// Considering optimistic-concurrency-control(OCC)
// Cosmosdb using _etag to accomplish optimistic-concurrency
// see https://docs.microsoft.com/en-us/azure/cosmos-db/sql/database-transactions-optimistic-concurrency#optimistic-concurrency-control
  
// When thread1 and thread2 partially update the same user1 with different fields.
// e.g:
// thread1. Partially update the "age" field to 20
db.updatePartial("Collection", user1.id, Map.of("age", 20), "Users");

// thread2. Partially update the "lastName" field to "Hanks"
db.updatePartial("Collection", user1.id, Map.of("lastName", "Hanks"), "Users");

// this will not throw a 412 Precondition Failed Exception.
// It is implemented by comparing the etag, if not match , read the original data, get the newest etag, and do merge and update again.


```



### Partial Update with Optimistic Concurrency Control (OCC)

```java
// For some reason(e.g. 2 threads may update the same field), if you want to obey the OCC rule and throw a 412 Precondition Failed Exception, you can set the following checkETag option to true and pass the previous etag.
// The checkETag option defaults to false, because when using partial update, in most cases we do not need an OCC.

// Hold the current etag.
var originData = db.read("Collection", user1.id, "Users").toMap();
var etag = originData.getOrDefault("_etag", "").toString();

// thread1. Partially update the "age" field to 20, "status" to "enabled", and add "_etag" for OCC.
db.updatePartial("Collection", user1.id, Map.of("age", 20, "status", "enabled", "_etag", etag), "Users", PartialUpdateOption.checkETag(true));

// thread2. Partially update the "lastName" field to "Hanks", "status" to "disabled", and add "_etag" for OCC.
// if thread2 is executed after thread1, a CosmosException with statusCode 412(Precondition Failed Exception) will be thrown.
db.updatePartial("Collection", user1.id, Map.of("lastName", "Hanks", "status", "disabled", "_etag", etag), "Users", PartialUpdateOption.checkETag(true));

```

### Patch (Similar to JSON Patch)

For details please refer to official SDK's [document](https://docs.microsoft.com/en-us/azure/cosmos-db/partial-document-update#supported-operations)

```java 
    var operations = PatchOperations.create()
                        // insert Golang at index 1
                        .add("/skills/1", "Golang") 
                        // set a new field
                        .set("/contents/sex", "Male"); 
    db.patch("Collection", id, operations, "Users");
```

The main difference between Partial update and Patch is that:

* Partial update is focused at insert/replace fields' values. And is able to support updating 10 more fields in one call.
* Patch is focused to implementing a method similar to JSON Patch, which supports more complicated ops like "Add, Set, Replace, Remove, Increment". And is not able to support ops exceeding 10 in one patch call.

### Complex queries

```java
    var cond=Condition.filter(
        "id","id010", // id equal to 'id010'
        "lastName","Banks", // last name equal to Banks
        "firstName !=","Andy", // not equal
        "firstName LIKE","%dy%", // see cosmosdb LIKE
        "location",List.of("New York","Paris"), // location is 'New York' or 'Paris'. see cosmosdb IN 
        "skills =",List.of("Java","React"), // skills equals array ["Java", "React"] exactly 
        "age >=",20, // see cosmosdb compare operators
        "firstName STARTSWITH","H", // see cosmosdb STARTSWITH
        "desciption CONTAINS","Project manager",// see cosmosdb CONTAINS
        "fullName.last RegexMatch","[A-Z]{1}ank\\w+$", // see cosmosdb RegexMatch
        "mail !=",Condition.key("mail2"), // using field a compares to field b
        "certificates IS_DEFINED",true, // see cosmosdb IS_DEFINED
        "families.villa IS_DEFINED",false, // see cosmosdb IS_DEFINED
        "age IS_NUMBER",true, // see cosmosdb type check functions
        "tagIds ARRAY_CONTAINS","T001", // see cosmosdb ARRAY_CONTAINS
        "tagIds ARRAY_CONTAINS_ANY",List.of("T001","T002"), // see cosmosdb EXISTS
        "tags ARRAY_CONTAINS_ALL name",List.of("Java","React"), // see cosmosdb EXISTS
        "$OR",List.of( // add an OR sub condition
        Condition.filter("position","leader"),  // subquery's fields/order/offset/limit will be ignored
        Condition.filter("organization.id","executive_committee")
        ),
        "$OR 2",List.of( // add another OR sub condition (name it $OR xxx in order to avoid the same key to a previous $OR )
        Condition.filter("position","leader"),  // subquery's fields/order/offset/limit will be ignored
        Condition.filter("organization.id","executive_committee")
        ),
        "$AND",List.of(
        Condition.filter("tagIds ARRAY_CONTAINS_ALL",List.of("T001","T002")).not() // A negative condition. see cosmosdb NOT
        Condition.filter("city","Tokyo")
        ),
        "$NOT",Map.of("lastName CONTAINS","Willington"), // A negative query using $NOT
        "$NOT 2",Map.of("$OR 3",  // A nested filter using $NOT and $OR
        List.of(
          Map.of("lastName", ""),  // note they will do the same thing using Condition.filter or Map.of
          Map.of("age >=", 20)
        )
      )
    )
    .fields("id", "lastName", "age", "organization.name") // select certain fields
    .sort("lastName", "ASC") //optional sort
    .offset(0) //optional offset
    .limit(100); //optional limit

    var users = db.find("Collection1", cond).toList(User.class);
```

### Aggregates

```java
    // support aggregate function: COUNT, AVG, SUM, MAX, MIN
    // see https://docs.microsoft.com/en-us/azure/cosmos-db/sql-query-aggregate-functions
    var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("location", "gender");

    var result = db.aggregate("Collection1", aggregate, Condition.filter("age >=", 20));

    // will generate a sql like this:
    /* SELECT COUNT(1) as facetCount, c.location, c.gender WHERE age >= 20 GROUP BY c.location, c.gender
    */
```

### Increment

```java
    // support incrementing a number field 
    // see https://docs.microsoft.com/en-us/azure/cosmos-db/partial-document-update-getting-started?tabs=java

    // increment age by 1. Supports int / long / double.
    var result = db.increment("Collection1", "id1", "/age", 1, "Users");

    // increment age by -5
    var result = db.increment("Collection1", "id1", "/age", -5, "Users");


		// 404 Not Found Exception will be throw if the id does not exist
    var result = db.increment("Collection1", "not exist id", "/name", 1, "Users");

    // 400 Bad Request Exception will be throw if the field is not an integer
    var result = db.increment("Collection1", "id1", "/name", 1, "Users");
    
```

### Cross-partition queries

```java
    // simple query
    var cond = Condition.filter("id", "M001").crossPartition(true);
    // when crossPartition is set to true, the partition filter will be ignored.
    var children = db.find(coll, cond);


    // aggregate query with cross-partition
    // This is current a limitation that you cannot write aggregate functions when using cross-partition. This will be resolved in later version.
    var aggregate = Aggregate.function("COUNT(1) as facetCount").groupBy("_partition");
    var cond = Condition.filter().crossPartition(true);
    var result = db.aggregate("Collection1", aggregate, cond);
```

### Raw SQL queries

```java
    // use raw sql for a complete query
    var queryText = "SELECT c.gender, c.grade\n" + 
    "    FROM Families f\n" + 
    "    JOIN c IN f.children WHERE f.address.state = @state ORDER BY f.id ASC";

        var params = new SqlParameterCollection(new SqlParameter("@state", "NY"));

        var cond = Condition.rawSql(queryText, params);
        var children = db.find(coll, cond, partition);

    // use raw sql as a where condition
    var cond = Condition.filter(SubConditionType.AND, List.of(
      Condition.filter("gender", "female"), 
      Condition.rawSql("1=1"));

    // use raw sql as a where condition with params
    var params =  new SqlParameterCollection(new SqlParameter("@state", "%NY%"));

    var cond = Condition.filter(
      "$AND", 
      List.of(
        Condition.filter("gender", "female"), 
        Condition.rawSql("c.state LIKE @state", params)
      ),
      "$AND another", // name it $AND xxx in order to avoid the same key to a previous $AND
      List.of(
        Condition.filter("age > ", "22"), 
        Condition.rawSql("c.address != \"\" ")
      )
     );      

    db.find("Collection1", cond, "Partition1");
```

### join queries

```java
    var cond = Condition.filter(
      "id", "id010", // id equal to 'id010'
      "lastName", "Banks", // last name equal to Banks
      "firstName !=", "Andy", // not equal
      "parents.firstName", "Tom", // parent is a array which will be join
      "$OR", List.of( // add an OR sub condition
        Condition.filter("parents.lastName", "Jones"),  // subquery's fields/order/offset/limit will be ignored
        Condition.filter("organization.id", "executive_committee")
      ),
      "$AND", List.of(// add an AND sub condition
        Condition.filter("parents.firstName", "Tom"),  
        Condition.filter("city", "Tokyo")
      ),
      "$NOT", Map.of("parents.lastName CONTAINS", "Willington"), // A negative query using $NOT
    )
    .fields("id", "lastName", "age", "organization.name") // select certain fields
    .join("parents") // the part which you want  to join
    .returnAllSubArray(false) // If select false, the result will filter the sub array which is not matched join condition. Default value is true.
    .sort("lastName", "ASC") //optional sort
    .offset(0) //optional offset
    .limit(100); //optional limit

    var users = db.find("Collection1", cond).toList(User.class);
```


## Reference

This library is built based on the official Azure Cosmos DB Java SDK v4.
https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-java-v4
