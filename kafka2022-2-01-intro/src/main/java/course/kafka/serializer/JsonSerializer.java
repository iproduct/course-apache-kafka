package course.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import course.kafka.exception.JsonSerializationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class JsonSerializer<T> implements Serializer<T> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.registerModule(new JavaTimeModule());
    }


    @Override
    public byte[] serialize(String topic, T entity) {
        try {
            return objectMapper.writeValueAsBytes(entity);
        } catch (JsonProcessingException ex) {
            log.error("Error serializing entity: " + entity, ex);
            throw new JsonSerializationException("Error serializing entity: "+ entity, ex);
        }
    }
}
