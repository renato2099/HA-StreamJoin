package ch.ethz.sjoin.consumer;

import ch.ethz.sjoin.producer.BidProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by marenato on 03.02.17.
 */
public class BidConsumer extends AbstractConsumer {

    private static Logger logger = LoggerFactory.getLogger(BidProducer.class);
    private static String DEF_GROUP_ID = "BidConsumerGroup";

    public BidConsumer() {
        super(BID_TOPIC, DEF_GROUP_ID);
    }

    public BidConsumer(String bidGroupId){
        super(BID_TOPIC, bidGroupId);
    }

    public static void main(String []args) {
        parseOptions(args);
        BidConsumer bc = new BidConsumer();
        logger.info(String.format("[%s] consuming from topic %s", bc.kafkaGroupId, bc.kafkaTopic));
        for (PartitionInfo partition: bc.getConsumer().partitionsFor(bc.kafkaTopic)) {
            logger.info(partition.toString());
        }

        while (true) {
            ConsumerRecords<String, String> records = bc.getConsumer().poll(100);
            if (records.count() > 0) {
                System.out.println("====>" + records.count());

                break;
            }
//            for (ConsumerRecord<String, String> record : records)
//
//                // print the offset,key and value for the consumer records.
//                System.out.printf("----->>>>>offset = %d, key = %s, value = %s\n",
//                        record.offset(), record.key(), record.value());
        }

    }

    public KafkaConsumer<String, String> getConsumer() {
        return this.consumer;
    }
}
