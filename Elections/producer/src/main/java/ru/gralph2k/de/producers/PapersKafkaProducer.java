package ru.gralph2k.de.producers;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.ElectionsProperties;
import ru.gralph2k.de.PaperSerializer;

import java.util.Properties;

public class PapersKafkaProducer extends PapersProducer {
    private static final Logger log = LoggerFactory.getLogger(PapersKafkaProducer.class);

    Producer<String, Object> kafkaProducer;

    PapersKafkaProducer(ElectionsProperties properties) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka_BOOTSTRAP_SERVERS_CONFIG());
        props.put(ProducerConfig.ACKS_CONFIG, properties.getKafka_ACKS_CONFIG());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PaperSerializer.class);
        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(props, new StringSerializer(), new PaperSerializer());
    }

    @Override
    public void send(String topic, String key, Object value) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(record);
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}