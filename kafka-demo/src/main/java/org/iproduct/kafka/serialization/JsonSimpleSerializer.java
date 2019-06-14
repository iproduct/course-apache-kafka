package org.iproduct.kafka.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.json.simple.JSONObject;

import java.util.Map;

@Slf4j
public class JsonSimpleSerializer implements Serializer<Object> {
    // instantiating ObjectMapper is expensive. In real life, prefer injecting the value.
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if(data instanceof Map) {
            Map map = (Map)data;
            JSONObject json = new JSONObject(map);
            return json.toJSONString().getBytes();
        } else {
            log.error(String.format("Simple json serialization is supported for Maps only. The supplied type %s is not a Map", data.getClass().getName()));
        }
        return "".getBytes();
    }

    @Override
    public void close() {
    }
}
