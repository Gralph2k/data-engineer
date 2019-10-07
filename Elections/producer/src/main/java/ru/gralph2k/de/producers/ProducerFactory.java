package ru.gralph2k.de.producers;

import ru.gralph2k.de.ElectionsProperties;

public class ProducerFactory {
    public static PapersProducer getInstance(String name, ElectionsProperties properties) {
        if (name.equals("PapersDummyProducer")) {
            return new PapersDummyProducer(properties);
        } else if (name.equals("PapersKafkaProducer")) {
            return new PapersKafkaProducer(properties);
        } else {
            return null;
        }
    }
}
