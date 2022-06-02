package course.kafka.util;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import java.util.Properties;

public class Temp {
    public static void main(String[] args) {
//        // Use the builders to define the actual processing topology, e.g. to specify
//        // from which input topics to read, which stream operations (filter, map, etc.)
//        // should be called, and so on.  We will cover this in detail in the subsequent
//        // sections of this Developer Guide.
//
//        StreamsBuilder builder = ...;  // when using the DSL
//        Topology topology = builder.build();
//        //
//        // OR
//        //
//        Topology topology = ...; // when using the Processor API
//
//        // Use the configuration to tell your application where the Kafka cluster is,
//        // which Serializers/Deserializers to use by default, to specify security settings,
//        // and so on.
//        Properties props = ...;
//
//        KafkaStreams streams = new KafkaStreams(topology, props);
//
//        // Add shutdown hook to stop the Kafka Streams threads.
//        // You can optionally provide a timeout to `close`.
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
