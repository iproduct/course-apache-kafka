package course.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

@Slf4j
public class TemperatureReadingConsumer implements Runnable {
    public static final String TOPIC = "temperature";
    public static final String CONSUMER_GROUP = "TemperatureEventsConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;

    private volatile boolean canceled;

    private static Consumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, String.class.getName());

        return new KafkaConsumer<String, String>(props);
    }

    public void cancel() {
        canceled = true;
    }

    @Override
    public void run() {
        try (var consumer = createConsumer()) {
            consumer.subscribe(List.of(TOPIC));
            while (!canceled) {
                var records = consumer.poll(
                        Duration.ofMillis(POLLING_DURATION_MS));
                for (var r : records) {
                    log.info("[Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, Leader Epoch: {}]: Key: {} --> Value: {}",
                            r.topic(), r.partition(), r.offset(), r.timestamp(), r.leaderEpoch(), r.key(), r.value());
                }
            }
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TemperatureReadingConsumer consumer = new TemperatureReadingConsumer();
        var executor = Executors.newCachedThreadPool();
        var producerFuture = executor.submit(consumer);
        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
        System.out.println("Closing the consumer ...");
        consumer.cancel();
        executor.shutdown();
    }
}
