package course.kafka.metrics;

import course.kafka.model.TemperatureReading;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class ProducerMetricsReporter implements Callable<String> {
    private final Producer<String, TemperatureReading> producer;

    public ProducerMetricsReporter(Producer<String, TemperatureReading> producer) {
        this.producer = producer;
    }

    // Filter all metrics by name
    private final Set<String> metricNames = Set.of(
            "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
            "request-size-max", "network-io-rate",
            "incoming-byte-rate", "batch-size-avg", "response-rate", "requests-in-flight," +
                    "network-io-total");


    @Override
    public String call() throws Exception {
        while (true) {
            final var metrics = producer.metrics();
            displayMetrics(metrics);
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException ex) {
                log.warn("Metrics interrupted for producer: " + producer);
                Thread.interrupted();
                break;
            }
        }
        return producer.toString();
    }

    private void displayMetrics(Map<MetricName, ? extends Metric> metrics) {
        final Map<String, MetricPair> metricsDisplay =
                metrics.entrySet().stream()
                        .filter(entry -> metricNames.contains(entry.getKey().name()))
                        .filter(entry -> {
                            var value = entry.getValue().metricValue();
                            if (!(value instanceof Double)) return false;
                            var doubleVal = (double) value;
                            return Double.isFinite(doubleVal) &&
                                    !Double.isNaN(doubleVal) &&
                                    doubleVal != 0;
                        }).map(entry -> new MetricPair(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toMap(MetricPair::toString, Function.identity(), (a, b) -> b, TreeMap::new));
        var sj = new StringJoiner("\n",
                "\n-------------------------------------------------------------------------------------------------------\n",
                "\n-------------------------------------------------------------------------------------------------------\n");
        metricsDisplay.forEach((name, pair) -> {
            sj.add(String.format("| %-40.40s | %-20.20s | %-10.2f | %-60.60s |",
                    name,
                    pair.getMetricName().name(),
                    (double) pair.getMetric().metricValue(),
                    pair.getMetricName().description()
                    ));
        });
        log.info(sj.toString());
    }
}
