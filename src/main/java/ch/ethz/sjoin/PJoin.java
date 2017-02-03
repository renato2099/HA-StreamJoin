package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AuctionConsumer;
import ch.ethz.sjoin.consumer.BidConsumer;

/**
 * Created by marenato on 03.02.17.
 */
public class PJoin {

    private BidConsumer bidCon;
    private AuctionConsumer aucCon;

    public PJoin() {
        bidCon = new BidConsumer();
        aucCon = new AuctionConsumer();
    }

    public static void main(String [] args) {
        // get each consumer
        PJoin pjoin = new PJoin();
        // do iterator model for consuming
        long procTuples = pjoin.startJoin();
        // keep both hashes in-memory
    }

    private long startJoin() {
        // have three threads
        // one each for polling from consumers
        // an extra one for joining
        return 0;
    }
}
