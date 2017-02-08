
package ch.ethz.haj.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.LoggerFactory;

import java.util.Vector;

/**
 * Created by marenato on 03.02.17.
 */
public class AuctionConsumer extends AbstractConsumer {
    // consumer group id
    private static String DEF_GROUP_ID = "AuctionConsumerGroup";

    /**
     * Default constructor
     */
    public AuctionConsumer() {
        super(AUCTION_TOPIC, DEF_GROUP_ID);
        this.logger = LoggerFactory.getLogger(AuctionConsumer.class);
    }

    public static void main(String[] args) {
        parseOptions(args);
        AuctionConsumer ac = new AuctionConsumer();
        ac.logger.info(String.format("[%s] consuming from topic %s", ac.kafkaGroupId, ac.kafkaTopic));
        for (PartitionInfo partition : ac.getConsumer().partitionsFor(ac.kafkaTopic)) {
            ac.logger.info(partition.toString());
        }

        while (true) {
            Vector<ConsumerRecord<Long, String>> records = ac.nextBatch();
            ac.printRecords(records);
        }
    }
}
