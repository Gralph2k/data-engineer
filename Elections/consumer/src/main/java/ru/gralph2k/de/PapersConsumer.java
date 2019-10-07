package ru.gralph2k.de;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.paperTypes.PaperType;
import ru.gralph2k.de.paperTypes.PaperType_Presidential_2018;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PapersConsumer {
    private static final Logger log = LoggerFactory.getLogger(PapersConsumer.class);

    String paperType;
    String topic;
    DbHelper dbHelper;

    public PapersConsumer(String topic, String paperType, String user, String password, String connectionString) {
        log.info("PapersConsumer created {}\n{}\n{}\n{}", topic, paperType, user, connectionString);
        this.topic = topic;
        this.paperType = paperType;
        this.dbHelper = new DbHelper(user, password, connectionString);
    }

    public void consume(ElectionsProperties electionsProperties) {
        Class paperTypeClass = PaperTypeFactory.getClass(paperType);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, electionsProperties.getKafka_BOOTSTRAP_SERVERS_CONFIG());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            paperTypeClass.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Выключить после отладки

        new PaperType_Presidential_2018(dbHelper).prepare();

        KafkaConsumer<String, PaperType_Presidential_2018> consumer =
            new KafkaConsumer(properties, new StringDeserializer(), new PaperDeserializer(paperTypeClass)); //TODO Вместо PaperType_Presidential_2018 использловать paperTypeClass. Разобраться с генериками
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, PaperType_Presidential_2018> records = consumer.poll(Duration.ofMillis(400));
            for (ConsumerRecord<String, PaperType_Presidential_2018> record : records) {
                log.debug("Received offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
                PaperType paperType = ((PaperType_Presidential_2018) record.value());
                paperType.setDbHelper(dbHelper); //TODO Отвратительно. Разделить бланк и логику работы с базой!
                paperType.save();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            log.info("Started. \nArgs.count={}", args.length);
            String propertiesFileName = "election.properties";
            if (args.length > 1) {
                log.info("Initializing consumer, sleeping for 30 seconds to let Kafka startup");
                Thread.sleep(30000);
                propertiesFileName = args[1];
            }

            ElectionsProperties properties = ElectionsProperties.getInstance(propertiesFileName);

            PapersConsumer papersConsumer = new PapersConsumer(
                properties.getTopic()
                , properties.getPaperTypeName()
                , properties.getConsumerPGUser()
                , properties.getConsumerPGPassword()
                , properties.getConsumerPGconnectionString());
            papersConsumer.consume(properties);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        log.info("Finished");
    }

}
