package ch.ethz.sj.ao;

import ch.ethz.sj.AbstractProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;

/**
 * Created by marenato on 02.02.17.
 */
public class AuctionProducer extends AbstractProducer {
    // Topic name
    public static final String AUCTION_TOPIC = "auction-topic";
    // missing partitions
    private static int missParts;
    // scaling factor
    private static int sf = 1;
    private static final int NUM_PARTS = 16;
    private static final long TUPLES_SF = 100;
    private static final double BEGIN_FAIL = 0.1;
    private static final double BEGIN_COMPLETION = 0.5;
    private static Logger logger = LoggerFactory.getLogger(AuctionProducer.class);
    private static Random random = new Random();
    private HashSet<Long> auctionsProduced;
    public AuctionProducer() {
        auctionsProduced = new HashSet<Long>();
    }

    public static void main(String[] args) {
        parseOptions(args);
        random.setSeed(1000L);
        logger.info(String.format("Producing %dK auction objects - Missing %d partitions.", sf, missParts));
        logger.info(String.format("Kafka:%s \t Zk:%s", kafkaUrl, zkUrl));
        AuctionProducer ap = new AuctionProducer();
        ap.initializeKafkaProducer(AUCTION_TOPIC, kafkaUrl, zkUrl);

        // generate random numbers and insert them
        long totTuples = sf * TUPLES_SF;
        long currTuples = 0;
        while (currTuples < totTuples) {
            Auction ao = ap.produceAuction(currTuples, totTuples);
            if (ao != null) {
                System.out.println(ao.toJson());
                ap.sendKafka((int)ao.getId()%NUM_PARTS, ao.getStrId(), ao.getTs(), ao.toJson());
            }
            //sendAuction(rAuction);
            if (ao != null && ao.getTs() > 0) {
                currTuples++;
            }
        }
    }

    private static void parseOptions(String[] args) {
        for (String opt : args) {
            String[] optParts = opt.split("=");
            AuctionOpts optOpts = AuctionOpts.valueOf(optParts[0].toLowerCase());
            ProducerOpts prodOpts = ProducerOpts.valueOf(optParts[0].toLowerCase());

            if (optOpts != null) {
                int optVal = Integer.parseInt(optParts[1]);
                switch (optOpts) {
                    case MISSING_PARTITIONS:
                        missParts = optVal;
                        break;
                    case SF:
                        sf = optVal;
                        break;
                }
            }
            if (prodOpts != null) {
                String val = optParts[1];
                switch (prodOpts) {
                    case KAFKA_URL:
                        kafkaUrl = val;
                        break;
                    case ZK:
                        zkUrl = val;
                        break;
                }
            }
        }
    }

    private Auction produceAuction(long currTuples, long totTuples) {
        Auction newAuction = null;
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

    // producer options
    public enum AuctionOpts {
        MISSING_PARTITIONS("missing"), SF("sf");
        String value;

        AuctionOpts(String v) {
            this.value = v;
        }
    }
}
