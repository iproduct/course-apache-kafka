package course.kafka.producer;

import course.kafka.model.TemperatureReading;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static course.kafka.model.TemperatureReading.HF_SENSOR_IDS;
import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;

@Slf4j
public class SimpleTemperatureReadingsProducer implements Callable<String> {
    public static final String TOPIC = "temperature";
    public static final String CLIENT_ID = "EventsClient";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    public static final String HIGH_FREQUENCY_SENSORS = "sensors.highfrequency";
    public static final int PRODUCER_TIMEOUT_MS = 20000;
    private String sensorId;
    private long maxDelayMs = 10000;
    private int numReadings = 10;

    public SimpleTemperatureReadingsProducer(String sensorId, long maxDelayMs, int numReadings) {
        this.sensorId = sensorId;
        this.maxDelayMs = maxDelayMs;
        this.numReadings = numReadings;
    }

    private static Producer<String, TemperatureReading> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // best combined with  min.insync.replicas > 1
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
//        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(HIGH_FREQUENCY_SENSORS, HF_SENSOR_IDS.stream().collect(Collectors.joining(",")));

        return new KafkaProducer<>(props);
    }

    @Override
    public String call() throws InterruptedException {
        var latch = new CountDownLatch(numReadings);
        try (var producer = createProducer()) {
            var count = new AtomicInteger();
            new Random().doubles(numReadings).map(t -> t * 40)
                    .peek(t -> {
                        try {
                            Thread.sleep((int) Math.random() * maxDelayMs);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .mapToObj(t -> new TemperatureReading(UUID.randomUUID().toString(), sensorId, t))
                    .map(reading -> new ProducerRecord(TOPIC, reading.getId(), reading))
                    .map(record -> {
                        return producer.send(record, ((metadata, exception) -> {
                            if (exception != null) {
                                log.error("Error sending temperature readings", exception);
                            }
                            log.info("SensorID: {}, Number: {}, Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                    sensorId, count.incrementAndGet(),
                                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                            latch.countDown();
                        }));
                    }).forEach(future ->{});
        }
        latch.await(PRODUCER_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return sensorId;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final List<SimpleTemperatureReadingsProducer> producers = new ArrayList<>();
        HF_SENSOR_IDS.forEach(hfs -> producers.add(new SimpleTemperatureReadingsProducer(hfs, 500, 20)));
        NORMAL_SENSOR_IDS.forEach(ns -> producers.add(new SimpleTemperatureReadingsProducer(ns, 5000, 2)));
        var executor = Executors.newCachedThreadPool();
        var ecs = new ExecutorCompletionService<String>(executor);
        producers.stream().forEach(producer -> ecs.submit(producer));
        for(int i = 0; i > producers.size(); i++){
            System.out.printf("!!! Sensor '%s' completed.", ecs.take().get());
        }

        executor.shutdownNow();
    }
}
