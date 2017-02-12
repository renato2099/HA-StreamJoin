package ch.ethz.haj.producer;

import ch.ethz.haj.model.Bid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.*;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by marenato on 02.02.17.
 */
public class BidProducer extends AbstractProducer {
    private static final int NUM_PARTS = 16;
    private static Logger logger = LoggerFactory.getLogger(BidProducer.class);
    private static Random random = new Random();

    /**
     * BidProducer constructor
     *
     * @param kafkaTopicName
     */
    public BidProducer(String kafkaTopicName) {
        super(kafkaTopicName);
    }

    /**
     * If there no missing partitions, then all will tuple insertions will succeed.
     * @param args
     */
    public static void main(String args[]) {
        parseOptions(args);
        random.setSeed(1000L);
        logger.info(String.format("Producing %dK auction objects - Missing %d partitions.", sf, missParts));
        logger.info(String.format("Kafka:%s \t Zk:%s", kafkaUrl, zkUrl));
        BidProducer bp = new BidProducer(BID_TOPIC);
        Set<Integer> partIds = chooseFailingPartitions(missParts);

        // generate bids uniformly distributed along the auctioned objects
        long nAuctionObjs = sf * tuplesSf;
        long totBids = nAuctionObjs * bidRatio;
        long currBids = 0, tProduced = 0, tFailed = 0;
        boolean fail;
        // select uniformly at random an object
        while (currBids < totBids) {
            long aucObjId = random.nextInt(Integer.MAX_VALUE) % nAuctionObjs;
            Bid bid = new Bid(currBids, aucObjId, random.nextDouble(), System.currentTimeMillis());
            // No punctuacted bid tuples are needed as we can just ignore them if there is no matching auctionObject.
            Integer bidPart = (int) currBids % NUM_PARTS;
            fail = false;
            if (currBids > totBids * pSuccess - 1) {
                //if (!partIds.contains(bidPart))
                if (bidPart < missParts) {
                    fail = true;
                    tFailed ++;
                }
            }
            if (!fail) {
                logger.debug(bid.toJson());
                bp.sendKafka(bidPart, bid.getId(), bid.getTs(), bid.toJson());
                tProduced ++;
            }
            currBids++;
        }
        logger.info(String.format("Tuples sent: %d\t Tuples failed: %d", tProduced, tFailed));
        bp.closeProducer();
    }

    private static Set<Integer> chooseFailingPartitions(int mPartitions) {
        Set<Integer> partIds = new HashSet<Integer>();
        while (mPartitions > 0) {
            int tmpSz = partIds.size();
            partIds.add(random.nextInt(NUM_PARTS));
            if (tmpSz != partIds.size())
                mPartitions--;
        }
        return partIds;
    }
}
