FROM gradle:8.4-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
# Χτίζουμε το jar και αγνοούμε τα τεστ για ταχύτητα
RUN gradle shadowJar --no-daemon

FROM eclipse-temurin:17-jre
EXPOSE 8080
# Αντιγράφουμε το σωστό αρχείο
COPY --from=build /home/gradle/src/build/libs/server-all.jar /app/server.jar
# Τρέχουμε τον σέρβερ
CMD ["java", "-jar", "/app/server.jar"]
