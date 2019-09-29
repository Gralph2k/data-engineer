package ru.gralph2k.de;

import org.testng.annotations.Test;

import java.sql.SQLException;


public class PapersConsumerTest {

    @Test
    public void testFileList() throws SQLException {

        String topic = "Presidential2018";
        String paperType = "PaperType_Presidential2018";
        String user = "consumer";
        String password = "goto@Postgres1";
        String connectionString = "jdbc:postgresql://localhost:5432/Elections";

        PapersConsumer papersConsumer = new PapersConsumer(topic, paperType, user, password, connectionString);

        papersConsumer.consume();
    }


}