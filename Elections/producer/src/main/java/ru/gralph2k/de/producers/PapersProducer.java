package ru.gralph2k.de.producers;

public interface PapersProducer {
    void send(String topic, String key, String value);
}
