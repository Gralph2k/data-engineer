package ru.gralph2k.de.producers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gralph2k.de.FileHelper;

public class PapersDummyProducer implements PapersProducer {
    private static final Logger log = LoggerFactory.getLogger(FileHelper.class);

    @Override
    public void send(String topic, String key, String value) {
        log.info("Send to topic: {}. Key:{}. Value:{}",topic,key,value);
    }
}
