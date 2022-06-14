package course.kafka.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class WordCountProcessor implements Processor<String, String, String, String> {
    private KeyValueStore<String, Long> kvStore;
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
//        context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
//            try (final KeyValueIterator<String, Long> iter = kvStore.all()) {
//                while (iter.hasNext()) {
//                    final KeyValue<String, Long> entry = iter.next();
//                    context.forward(new Record<>(
//                            entry.key,
//                            String.format("%-15s -> %4d", entry.key, entry.value),
//                            timestamp
//                    ));
//                }
//            }
//        });
        kvStore = context.getStateStore("inmemory-word-counts");
    }

    @Override
    public void process(Record<String, String> record) {
        final String[] words = record.value().toLowerCase().split("\\W+");

        for (final String word : words) {
            Long oldVal = kvStore.get(word);
            if (oldVal == null) {
                oldVal = 0L;
            }
            kvStore.put(word, oldVal + 1);
            context.forward(new Record<>(
                    word,
                    String.format("%-15s -> %4d", word, oldVal + 1),
                    record.timestamp()
            ));
        }
    }

    @Override
    public void close() {
    }

    import java.time.Duration;
    import org.apache.kafka.streams.kstream.TimeWindows;

    public static void main(String[] args) {
        // A tumbling time window with a size of 5 minutes (and, by definition, an implicit
        // advance interval of 5 minutes), and grace period of 1 minute.
        Duration windowSize = Duration.ofMinutes(5);
        Duration gracePeriod = Duration.ofMinutes(1);
        TimeWindows.ofSizeAndGrace(windowSize, gracePeriod);

        // The above is equivalent to the following code:
        TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(windowSize);
    }
}
