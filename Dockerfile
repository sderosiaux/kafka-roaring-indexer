FROM gradle:8-jdk21 AS build
WORKDIR /app
COPY . .
RUN gradle benchJar --no-daemon -q

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/build/libs/*-bench.jar app.jar
ENTRYPOINT ["java", "-Xms512m", "-Xmx2g", "-XX:+UseG1GC", "-jar", "app.jar"]
