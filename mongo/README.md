# MongoDB local dev

This folder contains a Docker Compose setup for a single-node MongoDB replica set used by local unit tests.

## Start

From the repository root:

```bash
cd mongo
docker-compose up -d
```

The setup starts:
- `mongo-replica` on `localhost:27017`
- `mongo-init-replica` to initialize replica set `rs0`

## Verify

```bash
docker-compose ps
docker logs mongo-init-replica
```

When initialization succeeds, logs include `Replica set initialized.`

## Local unit test

Then you can run the follow cmd or use an IDE to run unit tests.

```bash
mvn -Dtest='io.github.thunderz99.cosmos.impl.mongo.**.*Test' test
```

## Stop

```bash
docker-compose down
```
