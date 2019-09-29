package ru.gralph2k.de.producers;

abstract public class PapersProducer {
    abstract public void send(String topic, String key, Object value);
    abstract public void close();
}
