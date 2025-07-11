FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /build

COPY pom.xml .

RUN mvn -q dependency:go-offline -Dmaven.repo.local=/root/.m2

COPY src/main/scala/com/esgi/Producer          src/main/scala/com/esgi/Producer

RUN mvn -q package -Dmaven.test.skip=true -Dmaven.repo.local=/root/.m2
RUN ls -lh target

FROM eclipse-temurin:11-jre
WORKDIR /app

COPY --from=builder /build/target/spark-streaming-project-1.0-SNAPSHOT.jar /app/producer.jar


ENV JAVA_TOOL_OPTIONS="-Xms256m -Xmx512m"

ENTRYPOINT ["java","-jar","/app/producer.jar"]
