        FROM eclipse-temurin:17-jdk
        WORKDIR /app
        COPY . .
        RUN ./gradlew shadowJar --no-daemon
        EXPOSE 8080
        CMD ["java", "-jar", "build/libs/PlayWithMe-1.0-SNAPSHOT-all.jar"]
        
