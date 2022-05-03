package course.kafka.metrics;

import course.kafka.model.TemperatureReading;
import org.apache.kafka.clients.producer.Producer;

import java.util.Set;

public class ProducerMetricsReporter {
    private final Producer<String, TemperatureReading> producer;

    public ProducerMetricsReporter(Producer<String, TemperatureReading> producer) {
        this.producer = producer;
    }

    // Filter all metrics by name
    private final Set<String> metricNames = Set.of(
            "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
            "request-size-max", "network-io-rate", "record-queue-time-avg",
            "incoming-byte-rate", "batch-size-avg", "response-rate", "requests-in-flight");
}
