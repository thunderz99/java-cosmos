FROM postgres:16

# Install dependencies and add the PostgreSQL APT repository
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates gnupg lsb-release wget; \
    mkdir -p /etc/apt/keyrings; \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/pgdg.gpg; \
    echo "deb [signed-by=/etc/apt/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends postgresql-16-cron; \
    rm -rf /var/lib/apt/lists/*

# Copy an initialization script to enable the extension
COPY init-pgcron.sql /docker-entrypoint-initdb.d/
