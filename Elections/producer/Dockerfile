FROM openjdk:8
COPY ./target /usr/bin/app
COPY ./src/main/resources /usr/bin/app
RUN mkdir /Data
RUN mkdir /Data/Source
WORKDIR /usr/bin/app
ENTRYPOINT java -jar producer-1.0-SNAPSHOT-jar-with-dependencies.jar ru.gralph2k.de.PapersProcessor election.properties

