FROM openjdk:8
COPY ./target /usr/bin/app
COPY ./src/main/resources /usr/bin/app
WORKDIR /usr/bin/app
ENTRYPOINT java -jar consumer-1.0-SNAPSHOT-jar-with-dependencies.jar ru.gralph2k.de.PapersConsumer Presidential2018 PaperType_Presidential2018