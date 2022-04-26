package course.kafka.partitioner;

import course.kafka.model.TemperatureReading;
import course.kafka.producer.SimpleTemperatureReadingsProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static course.kafka.model.TemperatureReading.NORMAL_SENSOR_IDS;

public class TemperatureReadingsPartioner implements Partitioner {
    public static final int NUMBER_OF_PARTITIONS_PER_HF_SENSOR = 3;
    private List<String> highFrequencySensorIds;


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        final List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic(topic);
        final int partitionCount = partitionInfos.size();
        final int hfSensorsCount = highFrequencySensorIds.size();
        final int normalPartitionsCount = partitionCount - hfSensorsCount * NUMBER_OF_PARTITIONS_PER_HF_SENSOR;
        final String keyStr = key.toString();
        final TemperatureReading valueRading = (TemperatureReading) value;
        final String sensorId = valueRading.getSensorId();
        final int hfReadingIndex = highFrequencySensorIds.indexOf(sensorId);
        int hash = 0;
        if(hfReadingIndex >= 0) {
            hash = Integer.remainderUnsigned(Utils.murmur2(keyBytes), NUMBER_OF_PARTITIONS_PER_HF_SENSOR)
                    + hfReadingIndex * NUMBER_OF_PARTITIONS_PER_HF_SENSOR;
        } else {
            final int sensorIdIndex =  NORMAL_SENSOR_IDS.indexOf(sensorId);
            if(sensorIdIndex < 0){
                hash = partitionCount - 1; // redirect all unidentified sensor data to last partition
            } else {
                hash = hfSensorsCount * NUMBER_OF_PARTITIONS_PER_HF_SENSOR
                        + Integer.remainderUnsigned(sensorIdIndex, normalPartitionsCount);
            }
        }
        return hash;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        final String imortantSensorsStr = (String) configs.get(SimpleTemperatureReadingsProducer.HIGH_FREQUENCY_SENSORS);
        highFrequencySensorIds = Arrays.stream(imortantSensorsStr.split(",")).collect(Collectors.toUnmodifiableList());
    }
}
