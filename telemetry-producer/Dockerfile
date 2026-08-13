# Multi-stage Docker build for the Java Producer.
# 
# What it does:
# Stage 1 (builder): Uses Maven to compile the source code into a fat JAR.
# Stage 2 (runner): Copies the compiled JAR into a lightweight JRE image.
# 
# Why it exists:
# Ensures that the producer can be built and run on any machine without requiring the host 
# to have Java or Maven installed.
FROM maven:3.8-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -q -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/telemetry-producer-1.0-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]