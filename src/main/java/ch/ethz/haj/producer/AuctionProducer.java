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

    public long getRandomProducedId() {
        int sz = auctionsProduced.size();
        if (sz > 0) {
            int randEle = random.nextInt(sz);
            int i = 0;
            for (Long ele : auctionsProduced) {
                if (i == randEle)
                    return ele;
                i++;
            }
        }
        return -1L;
    }

    public static void main(String[] args) {
        parseOptions(args);
        random.setSeed(1000L);
        logger.info(String.format("Auction objects %d  - Missing %d partitions.", sf * tuplesSf, missParts));
        logger.info(String.format("CompleteAfter %f %% - FailAfter %f %%", pCompletion, pSuccess));
        logger.info(String.format("Kafka:%s \t Zk:%s", kafkaUrl, zkUrl));
        AuctionProducer ap = new AuctionProducer(AUCTION_TOPIC);

        // generate random numbers and insert them
        long totTups = sf * tuplesSf;
        long totUpds = (long) (totTups * UPD_PERCENTAGE);
        long tupToCmpl = (long) (totTups * INS_PERCENTAGE * pCompletion);
        long cntTups = 0, cmplTups = 0, upds = 0, nextId = 0, sent = 0, missed = 0;

        while (cntTups < totTups || tupToCmpl > cmplTups) {
            if (cntTups < totTups) {
                long ts = System.currentTimeMillis();
                long updId = ap.getRandomProducedId();
                boolean toUpd = random.nextBoolean() && upds < totUpds && updId >= 0;
                Auction ao;
                if (toUpd) {
                    ao = new Auction(updId, String.format("AuctionObject-%d", updId), ts);
                    logger.debug(String.format("New update %s", ao.toJson()));
                    upds++;
                } else {
                    ao = new Auction(nextId, String.format("AuctionObject-%d", nextId), ts);
                    ap.auctionsProduced.add(nextId);
                    logger.debug(String.format("New tuple: %s", ao.toJson()));
                    nextId++;
                }
                // send to kafka
                if (ap.sendToKafka(ao, totTups, cntTups)) missed++;
                else sent++;
                cntTups++;
            }

            // flip coin and start completing
            if (!ap.auctionsProduced.isEmpty() && random.nextBoolean()) {
                long idToComplete = ap.auctionsProduced.iterator().next();
                Auction compAuc = new Auction(idToComplete);
                compAuc.setTs(System.currentTimeMillis());
                ap.auctionsProduced.remove(idToComplete);
                logger.debug(String.format("Complete tuple: %s", compAuc.toJson()));
                // send to kafka
                if (ap.sendToKafka(compAuc, totTups, cntTups)) missed++;
                else sent++;
                cmplTups++;
            }
        }
        logger.info(String.format("TotTuples:%d\tNew:%d\tUpd:%d\tCompl:%d", cntTups, nextId, upds, tupToCmpl));
        logger.info(String.format("TupsSent:%d\tTupsMissed:%d", sent, missed));
        ap.closeProducer();
    }

    public boolean sendToKafka(Auction au, long totTups, long cntTups) {
        boolean toMiss = totTups * pSuccess < cntTups && au.getId() % NUM_PARTS < missParts;
        if (toMiss) {
            logger.debug("Missing tuple:" + au.toJson());
            // NOT TO DO: sendKafka((int)(au.getId()%NUM_PARTS), au.getId(), au.getTs(), au.toJson());
        } else {
            logger.debug("Sending tuple:" + au.toJson());
            sendKafka((int) (au.getId() % NUM_PARTS), au.getId(), au.getTs(), au.toJson());
        }
        return toMiss;
    }
}
