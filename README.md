# Jolie Execution Service

Allows users to write programs in the language [Jolie](https://jolie-lang.org) to run on their incoming messages.

## Prerequisites 

The service relies on a stack of tools to function properly.

* postgresql database system
* google cloud storage
* kafka

### Environment variables & Kubernetes yaml files

The service relies on several environment variables being present. It is advised to take a look at the kubernetes configuration files `kube/config.k8s.taml` and `kube/statefulset.k8s.yaml`, but we will cover most of them in detail under their respective section.

The kubernetes files are applied to the cluster through the command `kubectl apply -f {FILENAME}`, and is the only needed action to set up the service on kubernetes.
The CI/CD pipeline will then build and deploy to kubernetes on successful build.

### Postgres database

The service rely on an already created database. The following environment variables are expected:

* `DATABASE_HOST` IP / ExternalName of database system
* `DATABASE_PORT` Port to acces postgres on
* `JOLIE_EXEC_DB_NAME` Name of database the service should use (from `CREATE DATABASE` query)
* `POSTGRES_USER` Postgres user to login with, must have permission to create, read from and write to `JOLIE_EXEC_DB_NAME`
* `POSTGRES_PASSWORD` password to login to database with.

In this project, we set up a kubernetes secret `db-secrets` which securely stores all except database name.

`JOLIE_EXEC_DB_NAME` is defined in the kubernetes ConfigMap `kube/config.k8s.yaml`

### Google Cloud Storage

The service uses google cloud storage to retrieve user programs, which are expected to be uploaded by the webserver.

The google cloud storage bucket to be used can be configured through the env variable `JOLIE_EXEC_GCS_BUCKET_NAME`.

The credentials are stored as a secret file in kubernetes and mounted.
Take a look at `kube/statefulset.k8s.yaml` under `env` and `volumeMounts`, or follow the guide at the [google cloud storage docs](https://cloud.google.com/storage/docs/reference/libraries#setting_up_authentication).

### Kafka topics

#### Configuration Messages

To let the service know that a user adds or removes a script, configuration messages are used.

The configuration messages should be provided through the kafka topic `"jolie-exec-config-consumer-topic"`, as defined in the Kubernetes ConfigMap located in `kube/config.k8s.yaml`.

To register a script to be run on incoming messages for user with `userID=42`, a message similar to below should be sent:

```JSON
{
  "actionType": "enable",
  "userId": 42,
  "target": "recv"
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

#### User messages

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

----

## User Defined Jolie Scripts

### Limits

Due to large overhead when running jolie code (JVM) up to 8 user scripts can run simultaneously.
The jolie instances can use 8GB of ram in total and will be force killed after 15 seconds.

The Jolie programs will run without access to the internet.

### Minimal Example

A minimal example of a valid program can be found in `examples/minimal.ol`.

### API Legend

- `<MESSAGE_BODY>` is a string that should be able to hold messages of up to 10 MB
- `<USER_ID>` is the ID of a user
- `<ACTION>` is either `"forward"` or `"drop"`
- `...` denotes repition of the previous array element

### API for Outbound Message Scripts

Input is given as a JSON object via the first command line argument (`args[0]`) to user scripts for outbound messages. It has the following format:

```JSON
{
  "messageBody": <MESSAGE_BODY>,
  "ownID": <USER_ID>,
  "recipientIDs": [<USER_ID>, <USER_ID>]
}
```

The expected output for outbound message scripts is a JSON object written to `stdout`. It should look like this:

```JSON
{
  "messageBody": <MESSAGE_BODY>
}
```

### API for Inbound Message Scripts

Input is given as a JSON object via the first command line argument (`args[0]`) to user scripts for inbound messages. It has the following format:

```JSON
{
  "messageBody": <MESSAGE_BODY>,
  "ownID": <USER_ID>,
  "senderID": <USER_ID>,
  "recipientIDs": [<USER_ID>, ...]
}
```

The expected output for inbound message scripts is a JSON object written to `stdout`. It should look like this:

```JSON
{
  "action": <ACTION>,
  "reply": [
    {
      "to": <USER_ID>,
      "message": <MESSAGE_BODY>
    },
    ...
  ]
}
```
