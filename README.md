To run the test, run the following
```
mvn clean compile exec:java -Dexec.mainClass="com.datastax.test.Main" -DcontactPoints=<localdc-ip> -Dlocaldc=<localdc> -Dremotedc=<remotedc>
```
