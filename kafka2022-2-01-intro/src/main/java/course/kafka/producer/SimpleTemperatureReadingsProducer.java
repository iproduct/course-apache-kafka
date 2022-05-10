package course.kafka.producer;

import course.kafka.interceptor.CountingProducerInterceptor;
import course.kafka.metrics.ProducerMetricsReporter;
import course.kafka.model.TemperatureReading;
import course.kafka.partitioner.TemperatureReadingsPartioner;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static course.kafka.interceptor.CountingProducerInterceptor.REPORTING_WINDOW_SIZE_MS;
import static course.kafka.model.TemperatureReading.HF_SENSOR_IDS;
import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;

@Slf4j
public class SimpleTemperatureReadingsProducer implements Callable<String> {
    public static final String TOPIC = "temperature3";
    public static final String CLIENT_ID = "EventsClient";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    public static final int NUM_READINGS = 10;
    public static final String HIGH_FREQUENCY_SENSORS = "sensors.important";

    private String sensorId;
    private long delay_ms = 10000;
    private int numReadings = 10;
    private ExecutorService executor;
    private String transactionId = UUID.randomUUID().toString();

    public SimpleTemperatureReadingsProducer(String sensorId, long delay_ms, int numReadings, ExecutorService executor) {
        this.sensorId = sensorId;
        this.delay_ms = delay_ms;
        this.numReadings = numReadings;
        this.executor = executor;
    }

    private static Producer<String, TemperatureReading> createProducer(String transactionId) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TemperatureReadingsPartioner.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CountingProducerInterceptor.class.getName());
        props.put(REPORTING_WINDOW_SIZE_MS, 5000);
        props.put(HIGH_FREQUENCY_SENSORS, HF_SENSOR_IDS.stream().collect(Collectors.joining(",")));
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);

        return new KafkaProducer<>(props);
    }

    @Override
    public String call() {
        var latch = new CountDownLatch(numReadings);
        try (var producer = createProducer(transactionId)) {
            // Create metrics reporter for producer in separate thread
//            var reporterFuture = executor.submit(new ProducerMetricsReporter(producer));
            producer.initTransactions();
            var i = new AtomicInteger();
            try {
                var recordFutures = new Random().doubles(numReadings).map(t -> t * 40)
                        .peek(t -> {
                            try {
                                Thread.sleep(delay_ms);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            i.incrementAndGet();
                        })
                        .mapToObj(t -> new TemperatureReading(UUID.randomUUID().toString(), sensorId, t))
                        .map(reading -> new ProducerRecord(TOPIC, reading.getId(), reading))
                        .map(record -> {
                            return producer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    log.error("Error sending temperature readings", exception);
                                }
                                log.info("SENSOR_ID: {}, MESSAGE: {}, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                        sensorId, i.get(),
                                        metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                                latch.countDown();
                            });
                        }).collect(Collectors.toList());
                latch.await(15, TimeUnit.SECONDS);
                producer.commitTransaction();
                log.info("Transaction commited successfully [ID: {}]", transactionId);
            } catch (KafkaException kex) {
                log.error("Transaction [ID: " + transactionId + "] was ABORTED.", kex);
                producer.abortTransaction();
            }
//            finally {
//                // Cancel metrics reporter thread
//                log.info("Canceled the metrics reporter for [{}]: {}", producer, reporterFuture.cancel(true));
//            }

            log.info("!!! Closing producer for '{}'", sensorId);
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException ex) {
            log.error("Producer was unable to continue: ", ex);
        } catch (InterruptedException ie) {
            log.warn("Producer was interuped before completion: ", ie);
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
        return sensorId;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final List<SimpleTemperatureReadingsProducer> producers = new ArrayList<>();
        var executor = Executors.newCachedThreadPool();
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService(executor);
//        for (int i = 0; i < HF_SENSOR_IDS.size(); i++) {
//            var producer = new SimpleTemperatureReadingsProducer(HF_SENSOR_IDS.get(i), 250, 120, executor);
//            producers.add(producer);
//            ecs.submit(producer);
//        }
//        for (int i = 0; i < NORMAL_SENSOR_IDS.size(); i++) {
        for (int i = 0; i < 1; i++) {
            var producer = new SimpleTemperatureReadingsProducer(NORMAL_SENSOR_IDS.get(i), 1000, 3, executor);
            producers.add(producer);
            ecs.submit(producer);
        }
        for (int i = 0; i < producers.size(); i++) {
            System.out.printf("!!!!!!!!!!!! Producer for sensor '%s' COMPLETED.%n", ecs.take().get());
        }
        executor.shutdownNow();
    }
}
