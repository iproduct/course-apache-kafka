package org.iproduct.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class DemoProducer {
    private Properties kafkaProps = new Properties();
    private Producer producer;

    private class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            log.info("topic: {}, partition {}, offset {}, timestamp: {}",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            if (e != null) {
                e.printStackTrace();
            }
        }
    }

    public DemoProducer() {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.iproduct.kafka.serialization.JsonSimpleSerializer");
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    public void run() {
        for(int i = 0; i < 10; i++) {
            Map<String, String> data = new HashMap<>();
            data.put("title", "Test Title " + i);
            data.put("content", "Test content " + i + " ...");
            ProducerRecord<String, Map<String,String>> record =
                    new ProducerRecord<>("events", "Precision Products",data);
            try {
                producer.send(record, new DemoProducerCallback());//.get() for synchronous
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        DemoProducer demoProducer = new DemoProducer();
        demoProducer.run();
        Thread.sleep(5000);
    }
}
