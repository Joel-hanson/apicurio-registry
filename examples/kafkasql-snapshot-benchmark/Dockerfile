FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/kafkasql-snapshot-benchmark-1.0.0-SNAPSHOT.jar /app/benchmark.jar

# Defaults sized for a quick local demo (~16 MiB logical content).
# Override via docker-compose environment for larger runs.
ENV ROWS=2000 \
    CONTENT_BYTES=8192 \
    WARMUP=1 \
    ITERATIONS=3 \
    WORK_DIR=/tmp/bench-work

VOLUME ["/tmp/bench-work"]
ENTRYPOINT ["java", "-jar", "/app/benchmark.jar"]
