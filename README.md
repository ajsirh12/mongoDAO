# mongoDAO

## Setting mongodb driver
### Select 1. Referenced Libraries
- mongodb-driver-core-3.12.7.jar [[Download Link]](https://mvnrepository.com/artifact/org.mongodb/mongodb-driver-core/3.12.7)
- mongodb-driver-3.12.7.jar [[Download Link]](https://mvnrepository.com/artifact/org.mongodb/mongodb-driver/3.12.7)
- bson-3.12.7.jar [[Download Link]](https://mvnrepository.com/artifact/org.mongodb/bson/3.12.7)

### Select 2. Maven Repository
```
<!-- https://mvnrepository.com/artifact/org.mongodb/mongodb-driver-core -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver-core</artifactId>
    <version>3.12.7</version>
</dependency>

<!-- https://mvnrepository.com/artifact/org.mongodb/mongodb-driver -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>mongodb-driver</artifactId>
    <version>3.12.7</version>
</dependency>

<!-- https://mvnrepository.com/artifact/org.mongodb/bson -->
<dependency>
    <groupId>org.mongodb</groupId>
    <artifactId>bson</artifactId>
    <version>3.12.7</version>
</dependency>

```

## Setting ObjectMapper
### Select 1. Referenced Libraries
- jackson-core-asl-1.9.13.jar [[Download Link]](https://mvnrepository.com/artifact/org.codehaus.jackson/jackson-core-asl/1.9.13)
- jackson-mapper-asl-1.9.13.jar [[Download Link]](https://mvnrepository.com/artifact/org.codehaus.jackson/jackson-mapper-asl/1.9.13)

### Select 2. Maven Repository
```
<!-- https://mvnrepository.com/artifact/org.codehaus.jackson/jackson-core-asl -->
<dependency>
    <groupId>org.codehaus.jackson</groupId>
    <artifactId>jackson-core-asl</artifactId>
    <version>1.9.13</version>
</dependency>

<!-- https://mvnrepository.com/artifact/org.codehaus.jackson/jackson-mapper-asl -->
<dependency>
    <groupId>org.codehaus.jackson</groupId>
    <artifactId>jackson-mapper-asl</artifactId>
    <version>1.9.13</version>
</dependency>

```
## Usage Example
```
public class Application {

    public static void main(String[] args) {
        MongoDAO mongoDAO = new MongoDAO(URL, PORT, DB);
        mongoDAO.connectMongoDB();
		
        // mongoCRUD
        
        mongoDAO.disconnectMongoDB();
    }
}
```
