package course.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountProcessorDemo {
    public static void main(String[] args) {
        // 1) Configure stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Configure state store
        StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("WordCounts"),
                Serdes.String(),
                Serdes.Long());
//        KeyValueStore<String, Long> countStore = countStoreSupplier.build();


        // 2) Create topology builder
        final Topology topology = new Topology(); // build DAG
        topology.addSource("Source", "streams-input")
                .addProcessor("Process", () -> new WordCountProcessor(), "Source")
                .addStateStore(countStoreSupplier, "Process")
                .addSink("Sink","latest-word-counts", "Process");

        // 3) Build stream topology
        System.out.println(topology.describe());

        // 4) Create streams instance
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch Ctrl-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        // 5) Start streams and await termination
        try {
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
