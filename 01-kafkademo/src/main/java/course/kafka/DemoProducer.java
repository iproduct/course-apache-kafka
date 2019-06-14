package course.kafka;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class DemoProducer {
    private Properties kafkaProps = new Properties();
    private Producer producer;

    public DemoProducer() {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    public void run() {
        for(int i = 0; i < 10; i++) {
            ProducerRecord<String,String> record = new ProducerRecord<>("events",
                    "test-event-async" + i);
            Future<RecordMetadata> futureResult = producer.send(record,
                (metadata, exception) -> {
                    if(exception != null) {
                        log.error("Error publishing record: ", exception);
                        return;
                    }
                    log.info("topic: {}, partition {}, offset {}, timestamp: {}",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                });
//            try {
//                RecordMetadata metadata = futureResult.get();
//                log.info("topic: {}, partition {}, offset {}, timestamp: {}",
//                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        DemoProducer producer = new DemoProducer();
        producer.run();
        Thread.sleep(5000);
    }
}
