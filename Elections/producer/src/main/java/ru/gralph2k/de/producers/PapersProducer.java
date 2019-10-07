package ru.gralph2k.de.producers;

import java.io.Closeable;

abstract public class PapersProducer implements Closeable {
    abstract public void send(String topic, String key, Object value);
    abstract public void close();
}
