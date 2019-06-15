package course.kafka;

import course.kafka.model.Customer;
import course.kafka.model.StockPrice;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static course.kafka.StockPriceConstants.PRICES_TOPIC;

@Slf4j
public class StockPriceConsumer {
    private Properties props = new Properties();
    private KafkaConsumer<String, StockPrice> consumer;
    private Map<String, Integer> eventMap = new ConcurrentHashMap<>();

    public StockPriceConsumer() {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "stock-price-consumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "course.kafka.serialization.JsonDeserializer");
        props.put("value.deserializer.class", "course.kafka.model.StockPrice");
        props.put("enable.auto.commit", "false");
        consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(PRICES_TOPIC),
                new StockPriceRebalanceListener(consumer));
        while(true) {
            ConsumerRecords<String, StockPrice> records = consumer.poll(Duration.ofMillis(100));
            if(records.count() > 0) {
                log.info("Fetched {} records:", records.count());
                for (ConsumerRecord<String, StockPrice> rec : records) {
                    log.debug("Record - topic: {}, partition: {}, offset: {}, timestamp: {}, value: {}.",
                            rec.topic(), rec.partition(), rec.offset(), rec.timestamp(), rec.value());
                    log.info("{} -> {}, {}, {}, {}", rec.key(),
                            rec.value().getId(),
                            rec.value().getSymbol(),
                            rec.value().getName(),
                            rec.value().getPrice());
                    int updatedCount = 1;
                    if(eventMap.containsKey(rec.key())) {
                        updatedCount = eventMap.get(rec.key()) + 1;
                    }
                    eventMap.put(rec.key(), updatedCount);
                }

                JSONObject json = new JSONObject(eventMap);
                log.info(json.toJSONString());
            }
        }
    }

    public static void main(String[] args) {
        StockPriceConsumer demoConsumer = new StockPriceConsumer();
        demoConsumer.run();
    }
}
