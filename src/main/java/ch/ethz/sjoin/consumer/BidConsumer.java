package ch.ethz.sjoin.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.LoggerFactory;

/**
 * Created by marenato on 03.02.17.
 */
public class BidConsumer extends AbstractConsumer {
    // consumer group id
    private static String DEF_GROUP_ID = "BidConsumerGroup";

    /**
     * Default constructor
     */
    public BidConsumer() {
        super(BID_TOPIC, DEF_GROUP_ID);
        this.logger = LoggerFactory.getLogger(BidConsumer.class);
    }

    public BidConsumer(String bidGroupId) {
        super(BID_TOPIC, bidGroupId);
        this.logger = LoggerFactory.getLogger(BidConsumer.class);
    }

    public static void main(String[] args) {
        parseOptions(args);
        BidConsumer bc = new BidConsumer();
        bc.logger.info(String.format("[%s] consuming from topic %s", bc.kafkaGroupId, bc.kafkaTopic));
        for (PartitionInfo partition : bc.getConsumer().partitionsFor(bc.kafkaTopic)) {
            bc.logger.info(partition.toString());
        }

        while (true) {
            ConsumerRecords<Long, String> records = bc.nextBatch();
            bc.printRecords(records);
        }

    }
}
