    FROM gradle:8.4-jdk17 AS build
    COPY --chown=gradle:gradle . /home/gradle/src
    WORKDIR /home/gradle/src
    RUN gradle shadowJar --no-daemon

    FROM eclipse-temurin:17-jre
    EXPOSE 8080
    COPY --from=build /home/gradle/src/build/libs/*.jar /app/server.jar
    CMD ["java", "-jar", "/app/server.jar"]
    
