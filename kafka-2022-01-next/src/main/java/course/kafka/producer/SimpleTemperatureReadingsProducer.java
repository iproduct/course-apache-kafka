package course.kafka.producer;

import course.kafka.metrics.ProducerMetricsReporter;
import course.kafka.model.TemperatureReading;
import course.kafka.partitioner.TemperatureReadingsPartitioner;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static course.kafka.model.TemperatureReading.HF_SENSOR_IDS;
import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;

@Slf4j
public class SimpleTemperatureReadingsProducer implements Callable<String> {
//    public static final String TOPIC = "temperature3";
    public static final String TOPIC = "temperature";
    public static final String CLIENT_ID = "EventsClient_Group1";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    public static final String HIGH_FREQUENCY_SENSORS = "sensors.highfrequency";
    public static final int PRODUCER_TIMEOUT_SEC = 20;
    private String sensorId;
    private long maxDelayMs = 10000;
    private int numReadings = 10;
    private ExecutorService executor;

    public SimpleTemperatureReadingsProducer(String sensorId, long maxDelayMs, int numReadings, ExecutorService executor) {
        this.sensorId = sensorId;
        this.maxDelayMs = maxDelayMs;
        this.numReadings = numReadings;
        this.executor = executor;
    }

    private static Producer<String, TemperatureReading> createProducer() {
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
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, TemperatureReadingsPartitioner.class.getName());
        props.put(HIGH_FREQUENCY_SENSORS, HF_SENSOR_IDS.stream().collect(Collectors.joining(",")));
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "temperature-producer-1");

        return new KafkaProducer<>(props);
    }

    @Override
    public String call() {
        var latch = new CountDownLatch(numReadings);

        try (var producer = createProducer()) {
            //Run Metrics Producer Reporter which is runnable passing it the producer.
//            var reporterFuture = executor.submit(new ProducerMetricsReporter(producer));
            producer.initTransactions();
            try {
                producer.beginTransaction();
                var i = new AtomicInteger();
                var recordFutures = new Random().doubles(numReadings).map(t -> t * 40)
                        .peek(t -> {
                            try {
                                Thread.sleep((int) (Math.random() * maxDelayMs));
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

                producer.commitTransaction();
                log.error("Transaction COMMITED SUCCESSFULLY for SensorID: {}", sensorId);
            } catch (ProducerFencedException | OutOfOrderSequenceException |
                     AuthorizationException ex) {
                producer.close();
            } catch (KafkaException ex1) {
                log.error("Transaction unsuccessfull: ", ex1);
                producer.abortTransaction();
            }
            latch.await(15, TimeUnit.SECONDS);
//            reporterFuture.cancel(true);
            log.info("!!! Closing producer for '{}'", sensorId);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return sensorId;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final List<SimpleTemperatureReadingsProducer> producers = new ArrayList<>();
        var executor = Executors.newCachedThreadPool();
        ExecutorCompletionService<String> ecs = new ExecutorCompletionService(executor);
//        for (int i = 0; i < HF_SENSOR_IDS.size(); i++) {
//            var producer = new SimpleTemperatureReadingsProducer(HF_SENSOR_IDS.get(i), 500, 20, executor);
//            producers.add(producer);
//            ecs.submit(producer);
//        }

//        for (int i = 0; i < NORMAL_SENSOR_IDS.size(); i++) {
        for (int i = 0; i < 1; i++) {
            var producer = new SimpleTemperatureReadingsProducer(NORMAL_SENSOR_IDS.get(i), 5000, 10,executor);
            producers.add(producer);
            ecs.submit(producer);
        }
        for (int i = 0; i < producers.size(); i++) {
            System.out.printf("!!!!!!!!!!!! Producer for sensor '%s' COMPLETED.%n", ecs.take().get());
        }
        executor.shutdownNow();
    }
}
