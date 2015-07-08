To run the simple test with a 1KB payload, execute the following
```
mvn clean compile exec:java -Dexec.mainClass="com.datastax.test.Main" -DcontactPoints=<localdc-ip> -Dlocaldc=<localdc> -Dremotedc=<remotedc>
```

To use a larger 5MB payload, add the file argument, for example
```
mvn clean compile exec:java -Dexec.mainClass="com.datastax.test.Main" -DcontactPoints=<localdc-ip> -Dlocaldc=<localdc> -Dremotedc=<remotedc> -Dfile=bigfile5M
```

You can add pass any file to simulate your payload. 
