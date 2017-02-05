package ch.ethz.sjoin.producer;

import ch.ethz.sjoin.model.Bid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.*;
import java.util.Random;

/**
 * Created by marenato on 02.02.17.
 */
public class BidProducer extends AbstractProducer {

    private static final int BID_SF = 10;
    private static final int NUM_PARTS = 16;
    //    private static final double BEGIN_FAIL = 0.1;
//    private static final double BEGIN_COMPLETION = 0.5;
    private static Logger logger = LoggerFactory.getLogger(BidProducer.class);
    private static Random random = new Random();

    /**
     * BidProducer constructor
     * @param kafkaTopicName
     */
    public BidProducer(String kafkaTopicName) {
        super(kafkaTopicName);
    }

    public static void main(String args[]) {
        parseOptions(args);
        random.setSeed(1000L);
        logger.info(String.format("Producing %dK auction objects - Missing %d partitions.", sf, missParts));
        logger.info(String.format("Kafka:%s \t Zk:%s", kafkaUrl, zkUrl));
        BidProducer bp = new BidProducer(BID_TOPIC);

        // generate bids uniformly distributed along the auctioned objects
        long nAuctionObjs = sf * TUPLES_SF;
        long totBids = nAuctionObjs * BID_SF;
        long currBids = 0;
        // select uniformly at random an object
        while (currBids < totBids) {
            long aucObjId = random.nextInt(Integer.MAX_VALUE) % nAuctionObjs;
            Bid bid = new Bid(currBids, aucObjId, random.nextDouble(), System.currentTimeMillis());
            // No punctuacted bid tuples are needed as we can just ignore them if there is no matching auctionObject.
            //logger.debug(bid.toJson());
            bp.sendKafka((int) currBids % NUM_PARTS, bid.getId(), bid.getTs(), bid.toJson());
            currBids++;
        }
        bp.closeProducer();
    }
}
