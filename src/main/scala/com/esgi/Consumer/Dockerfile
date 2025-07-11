
FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /build

COPY pom.xml .
RUN mvn -q dependency:go-offline            -Dmaven.repo.local=/root/.m2
COPY src/main/scala/com/esgi/Consumer       src/main/scala/com/esgi/Consumer
RUN mvn -q package -Dmaven.test.skip=true   -Dmaven.repo.local=/root/.m2



FROM eclipse-temurin:11-jre
ARG SPARK_VERSION=3.5.0
WORKDIR /app

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

COPY --from=builder /build/target/spark-streaming-project-1.0-SNAPSHOT.jar consumer.jar


ENTRYPOINT ["spark-submit", \
    "--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0",\
    "--class","com.esgi.ConsumerKafka",\
    "--master","local[*]",\
    "/app/consumer.jar"]

