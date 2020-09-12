# java-cosmos: A lightweight Azure CosmosDB client for Java


java-cosmos is a client for Azure CosmosDB 's SQL API (also called documentdb formerly). Which is an opinionated library aimed at ease of use for CRUD and find (aka. query).

## Background
* Microsoft's official Java CosmosDB client is verbose to use

## Disclaimer
* This is an alpha version, and features are focused to CRUD and find at present.

## Quickstart

### Add dependency

```xml

<!-- Add a new repository in pom.xml -->

<repositories>
  <repository>
    <id>central</id>
    <name>bintray</name>
    <url>https://jcenter.bintray.com</url>
  </repository>
</repositories>

```

```xml

<!-- Add new dependency -->

<dependency>
  <groupId>io.github.thunderz99</groupId>
  <artifactId>java-cosmos</artifactId>
  <version>0.1.0</version>
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


## Examples

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
      "location",  List.of("New York", "Paris"), // location is 'New York' or 'Paris'. see cosmosdb IN 
      "age >=", 20, // see cosmosdb compare operators
      "middleName OR firstName STARTSWITH", "H", // see cosmosdb STARTSWITH
      "desciption CONTAINS", "Project manager",// see cosmosdb CONTAINS
      "skill ARRAY_CONTAINS", "Java" // see cosmosdb ARRAY_CONTAINS
    )
    .fields("id", "lastName", "age", "organization.name") // select certain fields
    .order("lastName", "ASC") //optional order
    .offset(0) //optional offset
    .limit(100); //optional limit
    
    var users = db.find("Collection1", cond).toList(User.class)
}


```
