package course.kafka.producer;

import course.kafka.model.TemperatureReading;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
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

@Slf4j
public class SimpleTemperatureReadingsProducer implements Runnable {
    public static final String TOPIC = "events";
    public static final String CLIENT_ID = "EventsClient";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<String, String>(props);
    }

    @Override
    public void run() {
        final var producer = createProducer();
        long time = System.currentTimeMillis();
        new Random().doubles(10).map(t -> t * 40)
                .mapToObj(t -> new TemperatureReading(UUID.randomUUID().toString(), "temperatureSensor01", t))
                .map(reading -> new ProducerRecord(TOPIC, reading.getId(), reading))
                .forEach(record -> {
                    try {
                        producer.send(record).get();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Error sending temperature reading", e);
                    }
                });
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SimpleTemperatureReadingsProducer producer = new SimpleTemperatureReadingsProducer();
        var executor = Executors.newCachedThreadPool();
        var producerFuture = executor.submit(producer);
        producerFuture.get();
        executor.shutdown();
    }
}
