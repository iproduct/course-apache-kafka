package course.kafka.producer;

import course.kafka.model.TemperatureReading;
import course.kafka.serialization.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static course.kafka.model.TemperatureReading.HF_SENSOR_IDS;

@Slf4j
public class SimpleTemperatureReadingsProducer implements Runnable {
    public static final String TOPIC = "temperature";
    public static final String CLIENT_ID = "EventsClient";
    public static final String BOOTSTRAP_SERVERS = "localhost:9093";
    public static final String HIGH_FREQUENCY_SENSORS = "sensors.highfrequency";

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
    public void run() {
        try (var producer = createProducer()) {
            long time = System.currentTimeMillis();
            var recordFutures = new Random().doubles(10).map(t -> t * 40)
                    .mapToObj(t -> new TemperatureReading(UUID.randomUUID().toString(), "temperatureSensor01", t))
                    .map(reading -> new ProducerRecord(TOPIC, reading.getId(), reading))
                    .map(record -> {
                        return producer.send(record, ((metadata, exception) -> {
                            if (exception != null) {
                                log.error("Error sending temperature readings", exception);
                            }
                            log.info("Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        }));
                    }).collect(Collectors.toList());
            recordFutures.forEach(f -> {
                try {
                    f.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error sending temperature readings", e);
                }
            });
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SimpleTemperatureReadingsProducer producer = new SimpleTemperatureReadingsProducer();
        var executor = Executors.newCachedThreadPool();
        var producerFuture = executor.submit(producer);
        producerFuture.get();
        executor.shutdownNow();
    }
}
