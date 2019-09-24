package ru.gralph2k.de.producers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.FileHelper;

public class PapersDummyProducer extends PapersProducer {
    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    PapersDummyProducer(){};

    @Override
    public void send(String topic, String key, Object value) {
        log.debug("Send to topic {}. Key:{}. Value:{}",topic,key,value);
    }
}
