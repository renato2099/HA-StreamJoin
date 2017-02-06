package ch.ethz.sjoin.consumer;

import ch.ethz.sjoin.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by marenato on 03.02.17.
 */
public class AbstractConsumer extends KafkaConfig {
    // default tuple batch size
    public static int BATCH_SIZE = 100;
    // logger
    public Logger logger;
    // Kafka consumer
    protected KafkaConsumer<Long, String> consumer;

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
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VAL_DESERIALIZER);
        // TODO this is not going to give us the best performance, change serializer
        this.consumer = new KafkaConsumer<Long, String>(properties);
        //this.consumer.
        this.consumer.subscribe(Arrays.asList(this.kafkaTopic));
    }

    public ConsumerRecords<Long, String> nextBatch() {
        ConsumerRecords<Long, String> recs = getConsumer().poll(BATCH_SIZE);
        logger.debug(String.format("%d records read from %s", recs.count(), this.kafkaTopic));
        return recs;
    }

    public KafkaConsumer<Long, String> getConsumer() {
        return this.consumer;
    }

    public void printRecords(ConsumerRecords<Long, String> records) {
        if (records.count() > 0) {
            System.out.println(String.format("Records received: %d", records.count()));
            for (ConsumerRecord<Long, String> record : records)
                // print the offset,key and value for the consumer records.
                System.out.printf("offset = %d, key = %d, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}
