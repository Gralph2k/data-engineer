FROM openjdk:8
COPY ./target /usr/bin/app
COPY ./src/main/resources /usr/bin/app
WORKDIR /usr/bin/app
ENTRYPOINT java -jar producer-1.0-SNAPSHOT-jar-with-dependencies.jar ru.gralph2k.de.PapersProcessor /Data/Source PapersKafkaProducer PaperType_Presidential_2018 Presidential_2018 10000

