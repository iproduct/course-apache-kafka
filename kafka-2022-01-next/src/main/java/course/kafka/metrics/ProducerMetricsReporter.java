package course.kafka.metrics;

import course.kafka.model.TemperatureReading;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class ProducerMetricsReporter implements Runnable {
    private final Producer<String, TemperatureReading> producer;
    //Used to Filter just the metrics we want
    private final Set<String> metricsNameFilter = Set.of(
            "record-queue-time-avg", "record-send-rate", "records-per-request-avg",
            "request-size-max", "network-io-rate", "record-queue-time-avg",
            "incoming-byte-rate", "batch-size-avg", "response-rate", "requests-in-flight"
    );


    public ProducerMetricsReporter(Producer<String, TemperatureReading> producer) {
        this.producer = producer;
    }

    @Override
    public void run() {
        while (true) {
            final Map<MetricName, ? extends Metric> metrics
                    = producer.metrics();

            displayMetrics(metrics);
            try {
                Thread.sleep(3_000);
            } catch (InterruptedException e) {
                log.warn("metrics interrupted");
                Thread.interrupted();
                break;
            }
        }
    }


    static class MetricPair {
        private final MetricName metricName;
        private final Metric metric;

        MetricPair(MetricName metricName, Metric metric) {
            this.metricName = metricName;
            this.metric = metric;
        }

        public String toString() {
            return metricName.group() + "." + metricName.name();
        }
    }

    private void displayMetrics(Map<MetricName, ? extends Metric> metrics) {
        final Map<String, MetricPair> metricsDisplayMap = metrics.entrySet().stream()
                //Filter out metrics not in metricsNameFilter
                .filter(metricNameEntry ->
                        metricsNameFilter.contains(metricNameEntry.getKey().name()))
                //Filter out metrics not in metricsNameFilter
                .filter(metricNameEntry -> {
                    var value = (double) metricNameEntry.getValue().metricValue();
                    return !Double.isInfinite(value) &&
                            !Double.isNaN(value) &&
                            value != 0;
                })
                //Turn Map<MetricName,Metric> into TreeMap<String, MetricPair>
                .map(entry -> new MetricPair(entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(
                        MetricPair::toString, Function.identity(), (a, b) -> a, TreeMap::new
                ));


        //Output metrics
        final StringBuilder builder = new StringBuilder(255);
        builder.append("\n---------------------------------------\n");
        metricsDisplayMap.entrySet().forEach(entry -> {
            MetricPair metricPair = entry.getValue();
            String name = entry.getKey();
            builder.append(String.format(Locale.US, "%50s%25s\t\t%,-10.2f\t\t%s\n",
                    name,
                    metricPair.metricName.name(),
                    metricPair.metric.metricValue(),
                    metricPair.metricName.description()
            ));
        });
        builder.append("\n---------------------------------------\n");
        log.info(builder.toString());
    }


}

