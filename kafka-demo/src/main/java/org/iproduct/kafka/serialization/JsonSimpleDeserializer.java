package org.iproduct.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JsonSimpleDeserializer implements Deserializer<Object> {
    // instantiating ObjectMapper is expensive. In real life, prefer injecting the value.
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        try {
            JSONParser parser = new JSONParser();
            return parser.parse(new String(bytes));
        } catch (ParseException e) {
            log.error(String.format("Json deserialization failed for object: %s", Map.class.getName()), e);
        }
        return new HashMap<>();
    }

    @Override
    public void close() {
    }
}
