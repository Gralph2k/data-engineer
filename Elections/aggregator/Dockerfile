FROM openjdk:8
COPY ./target /usr/bin/app
COPY ./src/main/resources /usr/bin/app
WORKDIR /usr/bin/app
ENTRYPOINT java -jar aggregator-1.0-SNAPSHOT-jar-with-dependencies.jar ru.gralph2k.de.PapersAggregator config/election.properties