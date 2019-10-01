package ru.gralph2k.de.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PaperSerializer implements Serializer {

    private static final Logger log = LoggerFactory.getLogger(PaperSerializer.class);

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
