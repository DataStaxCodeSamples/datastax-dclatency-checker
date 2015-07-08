To run the simple test with a 1KB payload and pause time of 1 second, execute the following
```
mvn clean compile exec:java -Dexec.mainClass="com.datastax.test.Main" -DcontactPoints=<localdc-ip> -Dlocaldc=<localdc> -Dremotedc=<remotedc>
```

To use a larger 5MB payload and change the pause time to 3 seconds, add the file and pauseInSeconds arguments, for example
```
mvn clean compile exec:java -Dexec.mainClass="com.datastax.test.Main" -DcontactPoints=<localdc-ip> -Dlocaldc=<localdc> -Dremotedc=<remotedc> -Dfile=bigfile5M -DpauseInSeconds=3
```

You can add pass any file to simulate your payload. 
