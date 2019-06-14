package course.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
public class DemoProducer {
    private Properties kafkaProps = new Properties();
    private Producer producer;

    public DemoProducer() {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "course.kafka.serialization.JsonSimpleSerializer");
        kafkaProps.put("acks", "all");
        kafkaProps.put("enable.idempotence", "true");
//        kafkaProps.put("max.request.size", "120");
        kafkaProps.put("transactional.id", "event-producer-1");
        kafkaProps.put("partitioner.class", "course.kafka.partitioner.SimplePartitioner");
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    public void run() {
        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                Map<String,String> customerData = new HashMap<>();
                customerData.put("customerId", "" + i);
                customerData.put("name", "customer-" + i);
                customerData.put("address", "address-" + i);
                int[] bulstat = new int[9];
                Arrays.fill(bulstat, i);
                customerData.put("eik", Arrays.asList(bulstat).stream().map(n -> "" + n)
                    .collect(Collectors.joining("")));

                ProducerRecord<String, Map<String,String>> record =
                        new ProducerRecord<>("events-replicated","" + i, customerData);
                Future<RecordMetadata> futureResult = producer.send(record,
                        (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Error publishing record: ", exception);
                                return;
                            }
                            log.info("topic: {}, partition {}, offset {}, timestamp: {}",
                                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        });
            }
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException ex) {
            producer.close();
        } catch (KafkaException ex1) {
            producer.abortTransaction();
        }
        producer.commitTransaction();
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        DemoProducer producer = new DemoProducer();
        producer.run();
        Thread.sleep(5000);
    }
}
