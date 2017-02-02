package ch.ethz.sj.bid;

import ch.ethz.sj.AbstractProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.*;
import java.util.Random;

/**
 * Created by marenato on 02.02.17.
 */
public class BidProducer extends AbstractProducer {
    // Topic name
    public static final String BID_TOPIC = "bid-topic";

    private static final int BID_SF = 10;
    private static final int NUM_PARTS = 16;
//    private static final double BEGIN_FAIL = 0.1;
//    private static final double BEGIN_COMPLETION = 0.5;
    private static Logger logger = LoggerFactory.getLogger(BidProducer.class);
    private static Random random = new Random();

    public static void main(String args[]) {
        parseOptions(args);
        random.setSeed(1000L);
        logger.info(String.format("Producing %dK auction objects - Missing %d partitions.", sf, missParts));
        logger.info(String.format("Kafka:%s \t Zk:%s", kafkaUrl, zkUrl));
        BidProducer bp = new BidProducer();
        bp.initializeKafkaProducer(BID_TOPIC, kafkaUrl, zkUrl);
        // generate bids uniformly distributed along the auctioned objects
        long nAuctionObjs = sf * TUPLES_SF;
        long totBids = nAuctionObjs * BID_SF;
        long currBids = 0;
        // select uniformly at random an object
        while (currBids < totBids) {
            Bid bid = new Bid(random.nextLong() % nAuctionObjs, random.nextDouble(), System.currentTimeMillis());
            // We don't need to send tuples to specify that no more bids will come, we can just ignore them.
            bp.sendKafka((int)bid.getObjId()%NUM_PARTS, bid.getStrObjId(), bid.getTs(), bid.toJson());
            currBids ++;
        }
    }
}
