package org.iproduct.kafka.serialization;

import ch.qos.logback.core.encoder.Encoder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.utils.VerifiableProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class JsonSerializer implements Serializer<Object> {
    // instantiating ObjectMapper is expensive. In real life, prefer injecting the value.
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error(String.format("Json serialization failed for object: %s", data.getClass().getName()), e);
        }
        return "".getBytes();
    }

    @Override
    public void close() {
    }
}
