package org.iproduct.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.json.simple.JSONObject;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DemoConsumer {
    private Properties props = new Properties();
    KafkaConsumer<String, Map<String, String>> consumer;
    Map<String, Integer> eventMap = new ConcurrentHashMap<String, Integer>();

    private class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            System.out.println(">>>" + recordMetadata);
            if (e != null) {
                e.printStackTrace();
            }
        }
    }

    public DemoConsumer() {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "EventCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.iproduct.kafka.serialization.JsonSimpleDeserializer");
        consumer = new KafkaConsumer<>(props);
    }

    public void run() {
        consumer.subscribe(Collections.singletonList("events"));
        try {
            while (true) {
                ConsumerRecords<String, Map<String, String>> records = consumer.poll(Duration.ofMillis(100));
                if (records.count() > 0) {
                    for (ConsumerRecord<String, Map<String, String>> record : records) {
                        log.debug("topic = {}, partition = {}, offset = {}, customer = {}, country = {}\n",
                                record.topic(), record.partition(), record.offset(),
                                record.key(), record.value());
                        int updatedCount = 1;
                        String title = record.value().get("title");
                        if (eventMap.containsKey(title)) {
                            updatedCount = eventMap.get(title) + 1;
                        }
                        log.info(">>> Record receved: {}", record.value());
                        eventMap.put(title, updatedCount);
                    }
                    JSONObject json = new JSONObject(eventMap);
                    System.out.println(">>>>>>>>>" + json.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        DemoConsumer demoProducer = new DemoConsumer();
        demoProducer.run();
        System.out.println("Press <Enter> to finish");
        Scanner sc = new Scanner(System.in);
        sc.nextLine();
    }
}
