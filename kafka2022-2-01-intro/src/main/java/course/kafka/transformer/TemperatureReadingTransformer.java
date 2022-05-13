package course.kafka.transformer;

import course.kafka.interceptor.CountingProducerInterceptor;
import course.kafka.model.TemperatureReading;
import course.kafka.serialization.JsonDeserializer;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static course.kafka.interceptor.CountingProducerInterceptor.REPORTING_WINDOW_SIZE_MS;

@Slf4j
public class TemperatureReadingTransformer implements Runnable {
    // Consumer constants
    public static final String IN_TOPIC = "temperature";
    ;
    public static final String CONSUMER_GROUP = "TemperatureTransformerConsumer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";//,localhost:9094,localhost:9095";
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";
    public static final long POLLING_DURATION_MS = 100;

    // Producer constants
    private static final String BASE_TRANSACTION_ID = "temperature-transformer-transaction-";
    public static final String OUT_TOPIC = "events";
    public static final String TRANSFORMER_PRODUCER_CLIENT_ID = "TemperatureReadingsProducer";

    //Consumer props
    private volatile boolean canceled;

    // Producer pros
    private String transactionId;

    public TemperatureReadingTransformer(String transactionId) {
        this.transactionId = transactionId;
    }

    private static Consumer<String, TemperatureReading> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase());
        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, TemperatureReading.class.getName());

        return new KafkaConsumer<>(props);
    }

    private static Producer<String, TemperatureReading> createProducer(String transactionId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, TRANSFORMER_PRODUCER_CLIENT_ID);
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
        props.put(REPORTING_WINDOW_SIZE_MS, 3000);
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
             var producer = createProducer(transactionId)) {
            consumer.subscribe(List.of(IN_TOPIC));
            var consumerGroupMetadata = consumer.groupMetadata();
            producer.initTransactions();

            while (!canceled) {
                var records = consumer.poll(
                        Duration.ofMillis(POLLING_DURATION_MS));
                for (var r : records) {
                    log.info("[Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, Leader Epoch: {}]: {} -->\n    {}",
                            r.topic(), r.partition(), r.offset(), r.timestamp(), r.leaderEpoch(), r.key(), r.value());
                    ProducerRecord<String, TemperatureReading> record =
                            new ProducerRecord<>(OUT_TOPIC, r.value().getId(), r.value());
                    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
                    currentOffsets.put(
                            new TopicPartition(r.topic(), r.partition()),
                            new OffsetAndMetadata(r.offset())
                    );
                    try {
                        producer.beginTransaction();
                        var metadata = producer.send(record).get();
                        producer.sendOffsetsToTransaction(currentOffsets, consumerGroupMetadata);
                        producer.commitTransaction();
                        log.info("Transaction COMMITTED successfully [ID: {}]", transactionId);
                        log.info("SENSOR_ID: {}, MESSAGE: {}, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                r.value().getSensorId(), i.get(),
                                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                    } catch (KafkaException kex) {
                        producer.abortTransaction();
                        log.error("Transaction [ID: " + transactionId + "] was ABORTED.", kex);
                    }
                }
            }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException |
                 ExecutionException ex) {
            log.error("Producer was unable to continue: ", ex);
        } catch (InterruptedException ie) {
            log.warn("Producer was interuped before completion: ", ie);
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var consumer = new TemperatureReadingTransformer(BASE_TRANSACTION_ID + 0);
        var executor = Executors.newCachedThreadPool();
        var producerFuture = executor.submit(consumer);
        System.out.println("Hit <Enter> to close.");
        new Scanner(System.in).nextLine();
        System.out.println("Closing the consumer ...");
        consumer.cancel();
        producerFuture.cancel(true);
        executor.shutdown();
    }
}
