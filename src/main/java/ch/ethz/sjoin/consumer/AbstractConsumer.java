package ch.ethz.sjoin.consumer;

import ch.ethz.sjoin.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by marenato on 03.02.17.
 */
public class AbstractConsumer extends KafkaConfig {
    // Kafka consumer
    protected KafkaConsumer<String, String> consumer;

    /**
     * Kafka consumer constructor
     */
    public AbstractConsumer(String kafkaTopic, String groupId) {
        this.kafkaTopic = kafkaTopic;
        this.kafkaGroupId = groupId;
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DESERIALIZER);
        // TODO this is not going to give us the best performance, change serializer
        this.consumer = new KafkaConsumer<String, String>(properties);
        //this.consumer.
        this.consumer.subscribe(Arrays.asList(this.kafkaTopic));
    }
}
