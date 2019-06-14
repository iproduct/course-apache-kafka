package course.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class DemoConsumer {
    private Properties props = new Properties();
    private KafkaConsumer<String, String> consumer;

    public DemoConsumer() {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "event-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList("events"));
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if(records.count() > 0) {
                log.info("Fetched {} records:", records.count());
                for (ConsumerRecord<String, String> rec : records) {
                    log.debug("Record - topic: {}, partition: {}, offset: {}, timestamp: {}.",
                            rec.topic(), rec.partition(), rec.offset(), rec.timestamp());
                    log.info("{} -> {}", rec.key(), rec.value());
                }
            }
        }
    }

    public static void main(String[] args) {
        DemoConsumer demoConsumer = new DemoConsumer();
        demoConsumer.run();
    }
}
