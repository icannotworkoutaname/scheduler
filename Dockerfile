# Build:  ./gradlew bootJar  &&  docker build -t chronos:latest .
# (single-stage on purpose — keeps the image build offline and fast; the jar
#  is already produced by the Gradle build the rest of the project uses.)
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/scheduler-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
