package course.kafka;

import course.kafka.model.TemperatureReading;
import course.kafka.serializer.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@Slf4j
public class SimpleTemperatureConsumer implements Runnable {
    public static final String TOPIC = "events";
    public static final String CONSUMER_GROUP = "EventsConsumer";
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "value.class";


    private volatile boolean canceled;

    public void cancel() {
        canceled = true;
    }

    private static Consumer<String, TemperatureReading> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, TemperatureReading.class.getName());

        return new KafkaConsumer<String, TemperatureReading>(props);
    }

    @Override
    public void run() {
        var consumer = createConsumer();
        consumer.subscribe(Collections.singletonList("events"));
        while (!canceled) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (var record : records) {
                log.info("Topic: {}, Partition: {}, Offset: {}, Timestamp: {} =>\nKey: {} -> Value: {}",
                        record.topic(), record.partition(), record.offset(), record.timestamp(),
                        record.key(), record.value());
            }
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SimpleTemperatureConsumer consumer = new SimpleTemperatureConsumer();
        var executor = Executors.newFixedThreadPool(1);
        var producerFuture = executor.submit(consumer);
        System.out.println("Hit <Enter> to cancel");
        new Scanner(System.in).nextLine();
        consumer.cancel();
        executor.shutdown();
    }
}
