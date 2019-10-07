package ru.gralph2k.de;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;


public class ElectionsProperties {
    private static final Logger log = LoggerFactory.getLogger(ElectionsProperties.class);

    private static ElectionsProperties single_instance = null;

    private String fileName;

    //Producer
    private String sourceDir; //Папка, из которой producer читает протоколы
    private String producerName; //Имя класса producer. Определяет формат передачи данных. PapersKafkaProducer по умолчанию.
    private String paperTypeName; //Имя класса, определяющего формат данных выборов (список кандидатов, метаданные и т.д.)
    private String topic; //Имя топика. Уникальное имя для каждых выборов //TODO Перенести в paperTypeName. Это метаданные выборов.
    private Integer producerProduceDelaySeconds; //Время задержки между обработкой протоколов.

    //Consumer
    private String consumerPGUser;
    private String consumerPGPassword;
    private String consumerPGconnectionString;

    //Aggregator
    private String aggregatorPGUser;
    private String aggregatorPGPassword;
    private String aggregatorPGconnectionString;
    private Integer aggregatorDelaySeconds; //Время задержки между выведением результатов выборов

    //Kafka
    private String kafka_BOOTSTRAP_SERVERS_CONFIG;
    private String kafka_ACKS_CONFIG;

    ElectionsProperties() {
    }

    public static ElectionsProperties getInstance(String fileName) throws IOException {
        if (single_instance == null) {
            single_instance = new ElectionsProperties();
            single_instance.fileName = fileName;
            single_instance.loadProperties();
        }
        return single_instance;
    }

    void loadProperties() throws IOException {
        File file = new File(this.fileName);
        Properties properties = new Properties();
        properties.load(new FileReader(file));

        sourceDir = properties.getProperty("sourceDir", "./Data/Source");
        producerName = properties.getProperty("producerName", "PapersKafkaProducer");
        paperTypeName = properties.getProperty("paperTypeName");
        topic = properties.getProperty("topic");
        producerProduceDelaySeconds = Integer.parseInt(properties.getProperty("producerDelaySeconds", "10"));

        consumerPGUser = properties.getProperty("consumerPGUser", "consumer");
        consumerPGPassword = properties.getProperty("consumerPGPassword");
        consumerPGconnectionString = properties.getProperty("consumerPGconnectionString", "jdbc:postgresql://$LOCAL_IP:5432/Elections");

        aggregatorPGUser = properties.getProperty("consumerPGUser", "consumer");
        aggregatorPGPassword = properties.getProperty("consumerPGPassword");
        aggregatorPGconnectionString = properties.getProperty("consumerPGconnectionString", "jdbc:postgresql://$LOCAL_IP:5432/Elections");
        aggregatorDelaySeconds = Integer.parseInt(properties.getProperty("aggregatorDelaySeconds", "5"));


        kafka_BOOTSTRAP_SERVERS_CONFIG = properties.getProperty("kafka.BOOTSTRAP_SERVERS_CONFIG");
        kafka_ACKS_CONFIG = properties.getProperty("kafka.ACKS_CONFIG", "all");
    }

    private String replaceIP(String value) {
        if (value.contains("$LOCAL_IP")) {
            try {
                //String localIp = InetAddress.getLocalHost().getCanonicalHostName();
                String localIp = "100.100.21.232"; //TODO Разрбраться как доставать localip
                if (localIp != null) {
                    return value.replace("$LOCAL_IP", localIp);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return value;
    }

    public String getFileName() {
        return fileName;
    }

    public String getSourceDir() {
        return sourceDir;
    }

    public String getProducerName() {
        return producerName;
    }

    public String getPaperTypeName() {
        return paperTypeName;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getProducerProduceDelaySeconds() {
        return producerProduceDelaySeconds;
    }

    public String getKafka_BOOTSTRAP_SERVERS_CONFIG() {
        return replaceIP(kafka_BOOTSTRAP_SERVERS_CONFIG);
    }

    public String getKafka_ACKS_CONFIG() {
        return kafka_ACKS_CONFIG;
    }

    public String getConsumerPGUser() {
        return consumerPGUser;
    }

    public String getConsumerPGPassword() {
        return consumerPGPassword;
    }

    public String getConsumerPGconnectionString() {
        return replaceIP(consumerPGconnectionString);
    }

    public String getAggregatorPGUser() {
        return aggregatorPGUser;
    }

    public String getAggregatorPGPassword() {
        return aggregatorPGPassword;
    }

    public String getAggregatorPGconnectionString() {
        return replaceIP(aggregatorPGconnectionString);
    }

    public Integer getAggregatorDelaySeconds() {
        return aggregatorDelaySeconds;
    }
}
