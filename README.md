# java-cosmos: A lightweight Azure CosmosDB client for Java


java-cosmos is a client for Azure CosmosDB 's SQL API (also called documentdb formerly). Which is an opinionated library aimed at ease of use for CRUD and find (aka. query).

## Background
* Microsoft's official Java CosmosDB client is verbose to use

## Disclaimer
* This is an alpha version, and features are focused to CRUD and find at present.

## Quickstart

### Add dependency

```xml

<!-- Add new dependency -->

<dependency>
  <groupId>com.github.thunderz99</groupId>
  <artifactId>java-cosmos</artifactId>
  <version>0.2.11</version>
</dependency>

```

### Start programming 

```java

import io.github.thunderz99.cosmos.Cosmos

public static void main(String[] args) {
    var db = new Cosmos(System.getenv("YOUR_CONNECTION_STRING")).getDatabase("Database1")
    db.upsert("Collection1", new User("id011", "Tom", "Banks"))
    
    var cond = Condition.filter(
      "id", "id010", // id equal to 'id010'
      "lastName", "Banks", // last name equal to Banks
      "firstName !=", "Andy", // not equal
    )
    .order("lastName", "ASC") //optional order
    .offset(0) //optional offset
    .limit(100); //optional limit
    
    var users = db.find("Collection1", cond).toList(User.class)
}

class User{

  public String id;
  public String firstName;
  public String lastName;
  public String location;
  public int age;
  
  public User(String id, String firstName, String lastName){
    this.id = id;
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
db.upsert("Collection1", new User("id011", "Tom", "Banks"), "Users");

// The default partition key is "_partition", so we'll get a json like this:
// {
//    "id": "id011",
//    "firstName": "Tom",
//    "lastName": "Banks",
//    "_partition": "Users"
// }

```

### Create database and collection dynamically

```java

var cosmos = new Cosmos(System.getenv("YOUR_CONNECTION_STRING"));
var db = cosmos.createIfNotExist("Database1", "Collection1");
db.upsert("Collection1", new User("id011", "Tom", "Banks"))

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

### Partial Update

```java

db.updatePartial("Collection", user1.id, Map.of("lastName", "UpdatedPartially"), "Users");


```



### Complex queries

```java
    
    var cond = Condition.filter(
      "id", "id010", // id equal to 'id010'
      "lastName", "Banks", // last name equal to Banks
      "firstName !=", "Andy", // not equal
      "firstName LIKE", "%dy%", // see cosmosdb LIKE
      "location",  List.of("New York", "Paris"), // location is 'New York' or 'Paris'. see cosmosdb IN 
      "skills =", List.of("Java", "React"), // skills equals array ["Java", "React"] exactly 
      "age >=", 20, // see cosmosdb compare operators
      "middleName OR firstName STARTSWITH", "H", // see cosmosdb STARTSWITH
      "desciption CONTAINS", "Project manager",// see cosmosdb CONTAINS
      "tagIds ARRAY_CONTAINS", "T001", // see cosmosdb ARRAY_CONTAINS
      "tagIds ARRAY_CONTAINS_ANY", List.of("T001", "T002"), // see cosmosdb EXISTS
      "tags ARRAY_CONTAINS_ALL name", List.of("Java", "React"), // see cosmosdb EXISTS
      "SUB_COND_OR", List.of( // add an OR sub condition
        Condition.filter("position", "leader"),  // subquery's fields/order/offset/limit will be ignored
        Condition.filter("organization.id", "executive_committee")
      ),
      "SUB_COND_OR 2", List.of( // add another OR sub condition (name it SUB_COND_OR xxx in order to avoid the same key to a previous SUB_COND_OR )
        Condition.filter("position", "leader"),  // subquery's fields/order/offset/limit will be ignored
        Condition.filter("organization.id", "executive_committee")
      ),
    )
    .fields("id", "lastName", "age", "organization.name") // select certain fields
    .order("lastName", "ASC") //optional order
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
      "SUB_COND_AND", 
      List.of(
        Condition.filter("gender", "female"), 
        Condition.rawSql("c.state LIKE @state", params)
      ),
      "SUB_COND_AND another", // name it SUB_COND_OR xxx in order to avoid the same key to a previous SUB_COND_AND
      List.of(
        Condition.filter("age > ", "22"), 
        Condition.rawSql("c.address != \"\" ")
      )
     );      
                                
    db.find("Collection1", cond, "Partition1");
                                
    
```

