package course.kafka.partitioner;

import course.kafka.model.StockPrice;
import course.kafka.model.TemperatureReading;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;
import static course.kafka.producer.SimpleTemperatureReadingsProducer.HIGH_FREQUENCY_SENSORS;
import static course.kafka.service.QuotesGenerator.STOCKS;

public class StockPricePartitioner implements Partitioner {
    public static final int NUMBER_OF_PARTITIONS_PER_HF_SENSOR = 3;
    private List<String> highFrequencySensorsIds;

    private static final Map<String, Integer> symbolToOrdinalMap = new HashMap<>();

    public StockPricePartitioner() {
        Flux.range(0, STOCKS.size()).zipWith(Flux.fromIterable(STOCKS))
                .subscribe((t) -> symbolToOrdinalMap.put(t.getT2().getSymbol(), t.getT1()));

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfos.size();
        final var keyStr = key.toString();
        return symbolToOrdinalMap.get(keyStr) % partitionCount;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
