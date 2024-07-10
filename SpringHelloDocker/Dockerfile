# Build jar: ./gradlew build
# Builder docker image: sudo docker build --build-arg JAR_FILE=./build/libs/SpringHello-0.0.1-SNAPSHOT.jar -t gbhat/springhello .
# Run docker: sudo docker run -p 8081:8080 gbhat/springhello
# Visit: http://localhost:8081/

FROM eclipse-temurin:17-jdk-alpine
VOLUME /tmp
ARG JAR_FILE
COPY ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
