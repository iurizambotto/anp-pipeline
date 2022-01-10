FROM postgres:latest
ENV POSTGRES_PASSWORD=secret
ENV POSTGRES_USER=username
ENV POSTGRES_DB=pipeline

COPY create_fixtures.sql /docker-entrypoint-initdb.d/create_fixtures.sql

