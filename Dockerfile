# Stage 1: Build
FROM gradle:8.4-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle shadowJar --no-daemon

# Stage 2: Run
FROM eclipse-temurin:17-jre
EXPOSE 8080
RUN mkdir /app
# Προσοχή: Εδώ διορθώνουμε τη διαδρομή για να βρει το αρχείο
COPY --from=build /home/gradle/src/build/libs/*.jar /app/server.jar
CMD ["java", "-jar", "/app/server.jar"]
