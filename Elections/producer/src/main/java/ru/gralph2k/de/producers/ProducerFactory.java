package ru.gralph2k.de.producers;

public class ProducerFactory {
    public static PapersProducer getInstance(String name) {
        if (name.equals("PapersDummyProducer")) {
            return new PapersDummyProducer();
        } else if (name.equals("PapersKafkaProducer")){
            return new PapersKafkaProducer();
        } else {
            return null;
        }
    }
}
