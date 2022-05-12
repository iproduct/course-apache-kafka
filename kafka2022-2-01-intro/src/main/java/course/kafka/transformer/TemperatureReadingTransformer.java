package course.kafka.transformer;

import course.kafka.interceptor.CountingProducerInterceptor;
import course.kafka.model.TemperatureReading;
import course.kafka.partitioner.TemperatureReadingsPartioner;
import course.kafka.serialization.JsonDeserializer;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static course.kafka.interceptor.CountingProducerInterceptor.REPORTING_WINDOW_SIZE_MS;
import static course.kafka.model.TemperatureReading.HF_SENSOR_IDS;

@Slf4j
public class TemperatureReadingTransformer implements Runnable {
    public static final String IN_TOPIC = "temperature";
    public static final String OUT_TOPIC = "events";
    public static final String PRODUCER_CLIENT_ID = "TemperatureTransformerProducer";
    public static final String CONSUMER_GROUP = "TemperatureTransformerConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093"; //,localhost:9093,localhost:9094";

    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;
    private String transactionId = UUID.randomUUID().toString();

    // producer props
    private long delay_ms = 10000;
    private int numReadings = 10;

    // consumer props
    private volatile boolean canceled;

    private static Consumer<String, TemperatureReading> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());

        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, TemperatureReading.class.getName());

        return new KafkaConsumer<>(props);
    }

    private static Producer<String, TemperatureReading> createProducer(String transactionId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingProducerInterceptor.class.getName());
        props.put(REPORTING_WINDOW_SIZE_MS, 5000);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

        return new KafkaProducer<>(props);
    }

    public void cancel() {
        canceled = true;
    }

    @Override
    public void run() {
        var i = new AtomicInteger();
        try (var consumer = createConsumer();
             var producer = createProducer(transactionId);
        ) {
            producer.initTransactions();
            consumer.subscribe(List.of(IN_TOPIC));
            while (!canceled) {
                //Read and validate deposits validated
                var records = consumer.poll(
                        Duration.ofMillis(POLLING_DURATION_MS));
                for (var r : records) {
                    log.info("Successfully CONSUMED: [Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, Leader Epoch: {}]: {} --> {}",
                            r.topic(), r.partition(), r.offset(), r.timestamp(), r.leaderEpoch(), r.key(), r.value());
                    //Send validated temperature readings & commit offsets atomically
                    try {
                        producer.beginTransaction();
                        var metadata = (RecordMetadata) producer.send(new ProducerRecord(OUT_TOPIC, r.value().getId(), r.value())).get();
                        // get current offsets
                        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
                        currentOffsets.put(new TopicPartition(r.topic(), r.partition()),
                                new OffsetAndMetadata(r.offset() + 1));
                        // commit offsets in transaction
                        producer.sendOffsetsToTransaction(currentOffsets, consumer.groupMetadata());
                        producer.commitTransaction();
                        log.info("Successfully SENT: [SENSOR_ID: {}, MESSAGE: {}, Topic: {}, Partition: {}, Offset: {}, Timestamp: {} -> {}C]",
                                r.value().getSensorId(), r.value().getId(),
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp(), r.value().getValue());
                    } catch (KafkaException kex) {
                        producer.abortTransaction();
                    }
                }
            }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException |
                 ExecutionException ex) {
            log.error("Producer was unable to continue: ", ex);
        } catch (
                InterruptedException ie) {
            log.warn("Producer was interuped before completion: ", ie);
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TemperatureReadingTransformer consumer = new TemperatureReadingTransformer();
        var executor = Executors.newCachedThreadPool();
        var producerFuture = executor.submit(consumer);
        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
        System.out.println("Closing the consumer ...");
        consumer.cancel();
        executor.shutdown();

    }
}

