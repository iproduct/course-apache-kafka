package org.iproduct.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JsonDeserializer implements Deserializer<Object> {
    // instantiating ObjectMapper is expensive. In real life, prefer injecting the value.
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Map.class);
        } catch (IOException e) {
            log.error(String.format("Json deserialization failed for object: %s", Map.class.getName()), e);
        }
        return new HashMap<>();
    }

    @Override
    public void close() {
    }
}
