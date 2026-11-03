ARG PG_MAJOR=17
FROM postgres:${PG_MAJOR}

# The official image sets PG_MAJOR itself, and its PGDG apt repo carries wal2json for every supported major.
RUN apt-get update \
    && apt-get -y install --no-install-recommends postgresql-${PG_MAJOR}-wal2json \
    && rm -rf /var/lib/apt/lists/*
