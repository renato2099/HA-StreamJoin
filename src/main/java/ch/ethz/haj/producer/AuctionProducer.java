package ch.ethz.haj.producer;

import ch.ethz.haj.model.Auction;
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
    private static Logger logger = LoggerFactory.getLogger(AuctionProducer.class);
    private static Random random = new Random();
    private HashSet<Long> auctionsProduced;

    /**
     * AuctionProducer constructor
     *
     * @param kafkaTopicName
     */
    public AuctionProducer(String kafkaTopicName) {
        super(kafkaTopicName);
        auctionsProduced = new HashSet<Long>();
    }

    public static void main(String[] args) {
        parseOptions(args);
        random.setSeed(1000L);
        logger.info(String.format("Auction objects %d  - Missing %d partitions.", sf * tuplesSf, missParts));
        logger.info(String.format("CompleteAfter %f %% - FailAfter %f %%", pCompletion, BEGIN_FAIL));
        logger.info(String.format("Kafka:%s \t Zk:%s", kafkaUrl, zkUrl));
        AuctionProducer ap = new AuctionProducer(AUCTION_TOPIC);

        // generate random numbers and insert them
        long totTuples = sf * tuplesSf;
        long tupsToCompl = (long) (totTuples * pCompletion);
        long currTuple = 0;
        while (currTuple < totTuples || tupsToCompl > 0) {
            Auction ao = ap.produceAuction(currTuple, tupsToCompl, totTuples);
            if (ao != null) {
                logger.debug(ao.toJson());
                // using key partitioning to ensure that all tuple updates are stored after their creation
                ap.sendKafka(ao.getId().intValue()%NUM_PARTS, ao.getId(), ao.getTs(), ao.toJson());
                if (ao.getInfo() == null) {
                    tupsToCompl--;
                } else {
                    currTuple++;
                }
            }
        }
        ap.closeProducer();
    }

    private Auction produceAuction(long currTuples, long tupsToCompl, long totTuples) {
        Auction newAuction = null;
        long currTs = System.currentTimeMillis();
        if (tupsToCompl > 0 && !this.auctionsProduced.isEmpty() && random.nextBoolean()) {
            long idToComplete = this.auctionsProduced.iterator().next();
            newAuction = new Auction(idToComplete);
            newAuction.setTs(currTs);
            this.auctionsProduced.remove(idToComplete);
        } else if (currTuples < totTuples) {
            newAuction = new Auction(currTuples);
            this.auctionsProduced.add(currTuples);
            newAuction.setInfo(String.format("AuctionObject-%d", currTuples));
            newAuction.setTs(currTs);
        }
        return newAuction;
    }
}