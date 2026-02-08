# PostgreSQL local dev

This folder contains a Docker Compose setup for local PostgreSQL used by unit tests.

The image is built from `postgres/Dockerfile`, which installs `postgresql-16-cron` and runs `init-pgcron.sql` to enable `pg_cron`.

## Start

From the repository root:

```bash
cd postgres
docker-compose up -d --build
```

Default connection details:
- Host: `localhost`
- Port: `5432`
- Database: `postgres`
- User: `postgres`
- Password: `postgres`

## Verify

```bash
docker-compose ps
docker exec -it postgres16 psql -U postgres -d postgres -c "SELECT extname FROM pg_extension WHERE extname='pg_cron';"
```

## Local unit test

Then you can run the follow cmd or use an IDE to run unit tests.

```bash
mvn -Dtest='io.github.thunderz99.cosmos.impl.postgres.**.*Test' test
```

## Stop

```bash
docker-compose down
```
