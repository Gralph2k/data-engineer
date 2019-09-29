package ru.gralph2k.de;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.paperTypes.PaperType_Presidential2018;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PapersConsumer {
    private static final Logger log = LoggerFactory.getLogger(PapersConsumer.class);

    String paperType;
    String topic;

    public PapersConsumer(String topic, String paperType){
        this.topic=topic;
        this.paperType=paperType;
    }

    private void consume() throws ClassNotFoundException{
        Class paperTypeClass = PaperTypeFactory.getClass(paperType);
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            paperTypeClass.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // TODO Выключить после отладки

        KafkaConsumer<String, PaperType_Presidential2018> consumer =
            new KafkaConsumer(properties,new StringDeserializer(), new PaperDeserializer(paperTypeClass)); //TODO Вместо PaperType_Presidential2018 использловать paperTypeClass. Как?
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, PaperType_Presidential2018> records = consumer.poll(Duration.ofMillis(400));
            for (ConsumerRecord<String, PaperType_Presidential2018> record : records) {
                log.info("Received offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
            }
        }
    }


    public static void main(String[] args) throws IOException,ClassNotFoundException{
        log.info("started. \nArgs.count={}",args.length);
        for (String arg:args) {
            log.info(arg);
        }

        String topic = "Presidential2018";
        String paperType = "PaperType_Presidential2018";


        if (args.length>=1) {
            topic = args[1];
        }
        if (args.length>=2) {
            paperType=args[2];
        }
        PapersConsumer papersConsumer = new PapersConsumer(topic,paperType);

        papersConsumer.consume();
        log.info("Finished");
    }




}
