package course.kafka.metrics;

import course.kafka.model.TemperatureReading;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ProducerMetricsReporter implements Callable<String> {
    private final Producer<String, TemperatureReading> producer;

    public ProducerMetricsReporter(Producer<String, TemperatureReading> producer) {
        this.producer = producer;
    }

    // Filter all metrics by name
    private final Set<String> metricNames = Set.of(
            "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
            "request-size-max", "network-io-rate", "record-queue-time-avg",
            "incoming-byte-rate", "batch-size-avg", "response-rate", "requests-in-flight");


    @Override
    public String call() throws Exception {
        while(true) {
            final var metrics = producer.metrics();
            displayMetrics(metrics);
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException ex){
                log.warn("Metrics interrupted for producer: " + producer);
                Thread.interrupted();
                break;
            }
        }
        return producer.toString();
    }

    private void displayMetrics(Map<MetricName,? extends Metric> metrics) {

    }
}
