package ch.ethz.sjoin.producer;

import ch.ethz.sjoin.model.Auction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;

/**
 * Created by marenato on 02.02.17.
 */
public class AuctionProducer extends AbstractProducer {

    private static final int NUM_PARTS = 16;
    // TODO do failures
    private static final double BEGIN_FAIL = 0.1;
    private static final double BEGIN_COMPLETION = 1.0;//0.5;
    private static Logger logger = LoggerFactory.getLogger(AuctionProducer.class);
    private static Random random = new Random();
    private HashSet<Long> auctionsProduced;

    /**
     * AuctionProducer constructor
     * @param kafkaTopicName
     */
    public AuctionProducer(String kafkaTopicName) {
        super(kafkaTopicName);
        auctionsProduced = new HashSet<Long>();
    }

    public static void main(String[] args) {
        parseOptions(args);
        random.setSeed(1000L);
        logger.info(String.format("Producing %dK auction objects - Missing %d partitions.", sf, missParts));
        logger.info(String.format("Kafka:%s \t Zk:%s", kafkaUrl, zkUrl));
        AuctionProducer ap = new AuctionProducer(AUCTION_TOPIC);

        // generate random numbers and insert them
        long totTuples = sf * tuplesSf;
        long currTuples = 0;
        while (currTuples < totTuples) {
            Auction ao = ap.produceAuction(currTuples, totTuples);
            if (ao != null) {
                logger.debug(ao.toJson());
                ap.sendKafka((int)ao.getId()%NUM_PARTS, ao.getId(), ao.getTs(), ao.toJson());
            }
            if (ao != null && ao.getTs() > 0) {
                currTuples++;
            }
        }
        ap.closeProducer();
    }

    private Auction produceAuction(long currTuples, long totTuples) {
        Auction newAuction;
        boolean complete = false;
        long currTs = System.currentTimeMillis();
        if (currTuples >= totTuples * BEGIN_COMPLETION) {
            // decide to complete or not
            complete = random.nextBoolean() && this.auctionsProduced.iterator().hasNext();
        }
        if (complete) {
            long idToComplete = this.auctionsProduced.iterator().next();
            newAuction = new Auction(idToComplete);
            newAuction.setTs(currTs);
            this.auctionsProduced.remove(idToComplete);
        }  else {
            newAuction = new Auction(currTuples);
            this.auctionsProduced.add(currTuples);
            newAuction.setInfo(String.format("AuctionObject-%d", currTuples));
            newAuction.setTs(currTs);
        }
        return newAuction;
    }
}
