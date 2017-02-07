package ch.ethz.sjoin.consumer;

import ch.ethz.sjoin.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by marenato on 03.02.17.
 */
public class AbstractConsumer extends KafkaConfig {

    public static int POLL_TIMEOUT = 5;
    // default tuple batch size
    public static int BATCH_SZ = 5;
    public Queue<ConsumerRecord<Long, String>> buffer;
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
        this.buffer = new ConcurrentLinkedQueue<ConsumerRecord<Long, String>>();
    }

    public Map<Long, String> nextBatch() {
        if (buffer.isEmpty() || buffer.size() < BATCH_SZ) {
            ConsumerRecords<Long, String> recs = getConsumer().poll(POLL_TIMEOUT);
            for (ConsumerRecord<Long, String> r : recs) {
                buffer.add(r);
            }
        }
        Map<Long, String> newBatch = new HashMap<Long, String>();
        int cnt = BATCH_SZ;
        while (cnt--> 0) {
            if (buffer.size() <= 0)
                return newBatch;
            ConsumerRecord<Long, String> r = buffer.poll();
            newBatch.put(r.key(), r.value());
        }
        logger.debug(String.format("%d records read from %s", newBatch.size(), this.kafkaTopic));
        return newBatch;
    }

    public KafkaConsumer<Long, String> getConsumer() {
        return this.consumer;
    }

    public void printRecords(Map<Long, String> records) {
        if (records.size() > 0) {
            logger.info(String.format("Records received: %d", records.size()));
            for (Map.Entry<Long, String> r : records.entrySet()) {
                // print the offset,key and value for the consumer records.
                logger.info(String.format("key = %d, value = %s", r.getKey(), r.getValue()));
                // System.out.printf("offset = %d, key = %d, value = %s\n", r.offset(), r.key(), r.value());
            }
        }
    }
}
