package course.kafka.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.processor.To;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class AdminClientDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        try (Admin admin = Admin.create(props)) {
            String topicName = "current-sales";
            int numPartitions = 12;
            short replicationFactor = 1;
            // Delete compacted topic if exists
            try {
                admin.deleteTopics(Collections.singleton(topicName)).all().get();
                log.info("Successfully deleted topic: {}", topicName);
            } catch (ExecutionException e) {
                log.error("Unable to delete topic: " + topicName, e);
            }
            // Check topic is deleted
            while (true) {
                log.info("Checking topic '{}' was successfully deleted.\nCURRENT TOPICS:", topicName);
                var topicsResult = admin.listTopics();
                var allTopics = topicsResult.listings().get();
                allTopics.forEach(System.out::println);
                System.out.println();
                if (!topicsResult.names().get().contains(topicName))
                    break;
                Thread.sleep(1000);
            }
            // Create compacted topic
            var result = admin.createTopics(
                    Collections.singleton(new NewTopic(topicName, numPartitions, replicationFactor)
                            .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                                    TopicConfig.DELETE_RETENTION_MS_CONFIG, "100",
                                    TopicConfig.SEGMENT_MS_CONFIG, "100",
                                    TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01",
                                    TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "100"))
                    )
            );
            var future = result.topicId(topicName);
            var topicUuid = future.get();
            log.info("Successfully created topic: {} [{}]", topicName, topicUuid);
            // Check topic is created
            while (true) {
                log.info("Checking topic '{}' was successfully created.\nCURRENT TOPICS:", topicName);
                var topicsResult = admin.listTopics();
                var allTopics = topicsResult.listings().get();
                allTopics.forEach(System.out::println);
                System.out.println();
                if (topicsResult.names().get().contains(topicName))
                    break;
                Thread.sleep(1000);
            }
            var cluster = admin.describeCluster();
            log.info("CLUSTER DESCRIPTION:\nID: {}\nController: {}\nNodes: {}\n",
                    cluster.clusterId().get(),
                    cluster.controller().get(),
                    cluster.nodes().get()
            );
            // Describe all topics
            var topicNames = admin.listTopics().names().get();
            admin.describeTopics(topicNames).topicNameValues()
                    .forEach((topic, topicDescriptionKafkaFuture) -> {
                        try {
                            log.info("Topic [{}] -> {}", topic, topicDescriptionKafkaFuture.get());
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        log.info("Demo complete.");
    }
}
