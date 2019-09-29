package ru.gralph2k.de.producers;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.FileHelper;
import ru.gralph2k.de.PaperSerializer;

import java.util.Properties;

public class PapersKafkaProducer extends PapersProducer {
    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    Producer<String, Object> kafkaProducer;

    PapersKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.103:9092"); //TODO  Вынести настройки из класса. Избавится от прописаного IP. Как?
        props.put(ProducerConfig.ACKS_CONFIG, "all");
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