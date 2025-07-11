# infra/kafka/Dockerfile  ────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ARG KAFKA_VERSION=3.5.1
ARG SCALA_VERSION=2.12
RUN curl -fsSL https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

ENV KAFKA_HOME=/opt/kafka
WORKDIR ${KAFKA_HOME}

COPY server.properties  config/server.properties

EXPOSE 9092

CMD ["bash","-c","exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties"]
