package course.kafka.interceptor;

import lombok.experimental.Helper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Records;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class CountingProducerInterceptor<K, V> implements ProducerInterceptor<K, V>, Runnable {
    public static final String REPORTING_WINDOW_SIZE_MS = "interceptor.reporting.window.size.ms";
    public static final long DEFAULT_REPORTING_WINDOW_SIZE_MS = 5000;
    private static ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private static Map<Set<Integer>, AtomicLong> sentMap = new ConcurrentHashMap<>();
    private static Map<Set<Integer>, AtomicLong> ackMap = new ConcurrentHashMap<>();
    private static Map<Set<Integer>, AtomicLong> errorMap = new ConcurrentHashMap<>();
    private Set<Integer> partitions = new ConcurrentSkipListSet<>();
    private AtomicLong numSent = new AtomicLong();
    private AtomicLong numAcked = new AtomicLong();
    private AtomicLong numErrors = new AtomicLong();

    private boolean scheduled;

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        var counter = sentMap.getOrDefault(partitions, new AtomicLong());
        counter.incrementAndGet();
        sentMap.putIfAbsent(partitions, counter);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        partitions.add(metadata.partition());

        if (exception != null) {
            ackMap.getOrDefault(partitions, new AtomicLong()).incrementAndGet();
        } else {
            errorMap.getOrDefault(partitions, new AtomicLong()).incrementAndGet();
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        long windowSize = DEFAULT_REPORTING_WINDOW_SIZE_MS;
        try {
            windowSize = Long.parseLong(String.valueOf(configs.get(REPORTING_WINDOW_SIZE_MS)));
        } catch (NumberFormatException ex) {
            log.error("Error: Invalid interceptor config property: " + REPORTING_WINDOW_SIZE_MS, ex);
        }
        synchronized(this){
            if(!scheduled) {
                scheduled = true;
                executor.scheduleAtFixedRate(this, windowSize, windowSize, TimeUnit.MILLISECONDS);
            }
        }

    }

    @Override
    public void run() {
        if(sentMap.get(partitions) != null && sentMap.get(partitions).get() != 0) {
            log.info(String.format("\t| Number Records Sent   | %15.15s | %10d |",
                    partitions, sentMap.get(partitions).get()));
        }
        sentMap.get(partitions).set(0);

//        log.info(String.format("\t| Number Records Acked  | %15.15s | %10d |",
//                partitions, numAcked.get()));
//        log.info(String.format("\t| Number Records Errors | %15.15s | %10d |",
//                partitions, numErrors.get()));
//        numSent.set(0);
//        numAcked.set(0);
//        numErrors.set(0);
    }
}
