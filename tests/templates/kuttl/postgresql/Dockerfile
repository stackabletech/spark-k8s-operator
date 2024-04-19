# docker build -t docker.stackable.tech/stackable/spark-k8s:3.5.1-stackable24.3.0-pgsql .
FROM docker.stackable.tech/stackable/spark-k8s:3.5.1-stackable24.3.0

RUN curl --fail -o /stackable/spark/jars/postgresql-42.6.0.jar "https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
