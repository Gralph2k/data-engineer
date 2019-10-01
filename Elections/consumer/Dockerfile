FROM openjdk:8
COPY ./target /usr/bin/app
COPY ./src/main/resources /usr/bin/app
WORKDIR /usr/bin/app
ENTRYPOINT java -jar consumer-1.0-SNAPSHOT-jar-with-dependencies.jar ru.gralph2k.de.PapersConsumer Presidential_2018 PaperType_Presidential_2018 consumer goto@Postgres1 jdbc:postgresql://100.100.21.232:5432/Elections