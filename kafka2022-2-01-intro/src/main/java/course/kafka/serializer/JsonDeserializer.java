package course.kafka.serializer;

import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import course.kafka.exception.JsonSerializationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Map;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "value.class";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    static {
        objectMapper.registerModule(new JavaTimeModule());
    }

    private Class<T> cls;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String configKey = isKey ? KEY_CLASS : VALUE_CLASS;
        String clsName = String.valueOf(configs.get(configKey));
        try {
            cls = (Class<T>) Class.forName(clsName);
        } catch (ClassNotFoundException e) {
            log.error("Failed to configure JsonDeserializer. " +
                    "Did you forget to specify the '{}' property?", configKey);
        }
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, cls);
        } catch (IOException ex) {
            String message = "";
            try {
                message = new String(data, "utf-8");
            } catch (UnsupportedEncodingException e) {
                log.error("Error decoding text to UTF-8", e);
            }
            log.error("Error serializing entity: " + message, ex);
            throw new JsonSerializationException("Error deserializing data: " + message, ex);
        }
    }

    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
