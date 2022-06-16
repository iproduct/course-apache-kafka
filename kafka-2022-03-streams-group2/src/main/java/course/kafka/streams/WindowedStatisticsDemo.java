package course.kafka.streams;

import course.kafka.model.CountSumAvg;
import course.kafka.model.TimestampedTemperatureReading;
import course.kafka.serialization.JsonDeserializer;
import course.kafka.serialization.JsonSerializer;
import course.kafka.util.CustomTimeExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.streams.kstream.Consumed.with;

public class WindowedStatisticsDemo {
    public static final String KEY_CLASS = "key.class";
    public static final String VALUE_CLASS = "values.class";

    public static final int WINDOW_SIZE_MS = 5000;


    public static void main(String[] args) {
        // 1) Configure stream
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "heating-biils");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once_v2");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimeExtractor.class.getName());

        props.put(KEY_CLASS, String.class.getName());
        props.put(VALUE_CLASS, TimestampedTemperatureReading.class.getName());

        // create JSON Serde
        Serde<TimestampedTemperatureReading> jsonSerde = Serdes.serdeFrom(new JsonSerializer(),
                new JsonDeserializer(TimestampedTemperatureReading.class));

        Serde<CountSumAvg> countSumJsonSerde = Serdes.serdeFrom(new JsonSerializer(),
                new JsonDeserializer(CountSumAvg.class));

        // 2) Create stream builder
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, TimestampedTemperatureReading> internalTempStream =
                builder.stream("temperature", with(Serdes.String(), jsonSerde));
        KStream<String, TimestampedTemperatureReading> externalTempStream = builder.stream("external-temperature");
        Predicate<String, TimestampedTemperatureReading> validTemperature =
                (sensorId, reading) -> reading.getValue() > -15 && reading.getValue() < 50;

        var readings = internalTempStream
                .filter(validTemperature)
                .mapValues((sensorId, reading) -> reading.getValue())
                .groupByKey(Grouped.valueSerde(Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(WINDOW_SIZE_MS)))
//                .count(Materialized.with(Serdes.String(), Serdes.Long()));
                .aggregate(() -> new CountSumAvg(0L, 0.0, 0.0),
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setSum(aggregate.getSum() + value);
                            aggregate.setAverage(aggregate.getSum() / aggregate.getCount());
                            return aggregate;
                        }, Materialized.with(Serdes.String(), countSumJsonSerde)
                );

        readings
                .toStream()
                .mapValues((sensorId, avgTemp) -> String.format("count:%4d, sum:%10.5f, average:%8.5f",
                        avgTemp.getCount(), avgTemp.getSum(), avgTemp.getAverage()))
                .to("events");

        // 3) Build stream topology
        final Topology topology = builder.build(); // build DAG
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
