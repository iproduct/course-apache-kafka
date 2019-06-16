package course.kafka;

import course.kafka.model.StockPrice;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;

import static course.kafka.StockPriceConstants.PRICES_TOPIC;

public class StockPriceRebalanceListener  implements ConsumerRebalanceListener {
    KafkaConsumer<String, StockPrice> consumer;

    public StockPriceRebalanceListener(KafkaConsumer<String, StockPrice> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//        consumer.seekToBeginning(partitions);

    }
}
