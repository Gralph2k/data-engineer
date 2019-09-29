package ru.gralph2k.de;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.paperTypes.PaperType_Presidential2018;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PapersConsumer {
    private static final Logger log = LoggerFactory.getLogger(PapersConsumer.class);

    String paperType;
    String topic;
    DbHelper dbHelper;

    public PapersConsumer(String topic, String paperType, String user, String password, String connectionString) {
        this.topic = topic;
        this.paperType = paperType;
        this.dbHelper = new DbHelper(user, password, connectionString);
    }

    public void consume() throws SQLException {
        Class paperTypeClass = PaperTypeFactory.getClass(paperType);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.103:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            paperTypeClass.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // TODO Выключить после отладки

        PaperType_Presidential2018.prepare(dbHelper);

        KafkaConsumer<String, PaperType_Presidential2018> consumer =
            new KafkaConsumer(properties, new StringDeserializer(), new PaperDeserializer(paperTypeClass)); //TODO Вместо PaperType_Presidential2018 использловать paperTypeClass. Как?
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, PaperType_Presidential2018> records = consumer.poll(Duration.ofMillis(400));
            for (ConsumerRecord<String, PaperType_Presidential2018> record : records) {
                log.info("Received offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
                ((PaperType_Presidential2018) record.value()).save(dbHelper);
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        log.info("started. \nArgs.count={}", args.length);
        for (String arg : args) {
            log.info(arg);
        }

        String topic = "Presidential2018";
        String paperType = "PaperType_Presidential2018";
        String user = "consumer";
        String password = "goto@Postgres1";
        String connectionString = "jdbc:postgresql://localhost:5432/Elections";

        //TODO Заменить на cli
        if (args.length >= 1) {
            topic = args[1];
        }
        if (args.length >= 2) {
            paperType = args[2];
        }
        if (args.length >= 3) {
            user = args[3];
        }
        if (args.length >= 4) {
            password = args[4];
        }
        if (args.length >= 5) {
            connectionString = args[5];
        }
        PapersConsumer papersConsumer = new PapersConsumer(topic, paperType, user, password, connectionString);

        papersConsumer.consume();
        log.info("Finished");
    }


}
