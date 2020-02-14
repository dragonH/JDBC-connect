# JDBC-connect
A AWS lambda that can test JDBC connection and get JDBC tables

# Request payload
```
{
    String endPoint;
    String user;
    String password;
    String databaseName;
    String databaseType;
}
```

# Response payload
```
    Boolean result;
    String message;
    List<String> tables;
```