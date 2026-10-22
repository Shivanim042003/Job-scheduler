FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
RUN useradd --system --create-home scheduler
WORKDIR /app
COPY --from=build /app/target/job-scheduler-*.jar app.jar
USER scheduler
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
