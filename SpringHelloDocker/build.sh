export JAVA_HOME=/home/guru/bin/jdk-17.0.7/
JAVA_HOME=/home/guru/bin/jdk-17.0.7/
./gradlew clean build
sudo docker build --build-arg JAR_FILE=./build/libs/SpringHello-0.0.1-SNAPSHOT.jar -t gbhat/springhello .

