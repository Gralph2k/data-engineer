package ru.gralph2k.de.producers;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.FileHelper;
import ru.gralph2k.de.serializer.PaperSerializer;


import java.util.Properties;

public class PapersKafkaProducer extends PapersProducer {
    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    Producer<String, Object> kafkaProducer;

    PapersKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092,kafka-2:9092,kafka-3:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "ru.gralph2k.de.serializer.PaperSerializer");
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(props, new StringSerializer(), new PaperSerializer());
    }

    @Override
    public void send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(record);
        log.debug("Send to topic {}. Key:{}. Value:{}", topic, key, value);
    }

}