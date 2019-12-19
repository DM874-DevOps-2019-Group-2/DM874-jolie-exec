# Jolie Execution Service

Allows users to write programs in the language [Jolie](https://jolie-lang.org) to run on their incoming messages.


## Configuration (Kafka)
The service is configured through the apache-kafka topic given through the environment variable `JOLIE_EXEC_CONFIG_TOPIC`

### Configuration Messages
```JSON
{
  "messageType": add,
  "userID": 42,
  "target": "recv",
}
```

```JSON
{
  "messageType": remove,
  "userID": 42,
  "target": "recv"
}
```


## Notes
The user programs are limited through prlimit
e.g. `prlimit --rss=536870912 --cpu=10 --pid <pid>` would limit the memory usage to 512MB and cpu time to 10 seconds
