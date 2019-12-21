# Jolie Execution Service

Allows users to write programs in the language [Jolie](https://jolie-lang.org) to run on their incoming messages.

## Kafka topics

### Configuration Messages

To let the service know that a user adds or removes a script, configuration messages are used.

The configuration messages should be provided through the kafka topic `"jolie-exec-config-consumer-topic"`, as defined in the Kubernetes ConfigMap located in `kube/config.k8s.yaml`.


To register a script to be run on incoming messages for user with `userID=42`, a message similar to below should be sent:

```JSON
{
  "actionType": "enable",
  "userId": 42,
  "target": "recv",
}
```

To disable the script run on incoming messages for user with `userID=42`, a message similar to the following should be sent:

```JSON
{
  "actionType": "disable",
  "userId": 42,
  "target": "recv"
}
```

In the future, replacing `"recv"` with `"send"` should allow control of scripts run on outgoing messages.

___

### User messages

The user messages should be provided through the kafka topic `"jolie-exec-consumer-topic"`, as defined in the Kubernetes ConfigMap located in `kube/config.k8s.yaml`. The expected input is consistent with that described in `DM874-report/desc.md`:

```JSON
{
  "messageUid": "c0a630d2-8db3-4a03-9e19-7141582f37aa",
  "sessionUid": "cf2bd7ca-ba13-40d9-8fb7-bab2064028d4",
  "messageBody": "Hello, world!",
  "senderId": 42,
  "recipientIds": [12, 8],
  "fromAutoReply": false,
  "eventDestinations": ["TOPIC1", "TOPIC2"]
}
```

## Notes

### Program limits

Due to large overhead when running jolie code (JVM) up to 8 user scripts can run simultaneously.
The jolie instances can use 8GB of ram in total and will be force killed after 1 minute of CPU time.

The Jolie programs will run without access to the internet.

### Minimal example

A minimal example of a valid program can be found in `examples/minimal.ol`.
