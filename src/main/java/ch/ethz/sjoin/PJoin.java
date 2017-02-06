package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AbstractConsumer;
import ch.ethz.sjoin.consumer.AuctionConsumer;
import ch.ethz.sjoin.consumer.BidConsumer;
import ch.ethz.sjoin.model.Bid;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by marenato on 03.02.17.
 */
public class PJoin {

    private AbstractConsumer relACon;
    private AbstractConsumer relBCon;
    private ConcurrentHashMap<Long, Set<Bid>> joinState;
    private ConcurrentHashMap<Long, Long> relA;
    private ConcurrentHashMap<Long, Bid> relB;
    // thread for consuming data
    private TConsumer tCon;
    // thread for consuming and probing data
    private TProber tProbe;
    // thread for garbage collect joinState
    private TCollector tCollector;

    public PJoin(AbstractConsumer auctionConsumer, AbstractConsumer bidConsumer) {
        relACon = auctionConsumer;
        relBCon = bidConsumer;
        tCon = new TConsumer(auctionConsumer, relA);
        tProbe = new TProber(bidConsumer, relA, relB, joinState);
        tCollector = new TCollector(joinState);
        joinState = new ConcurrentHashMap<Long, Set<Bid>>();
        relA = new ConcurrentHashMap<Long, Long>();
        relB = new ConcurrentHashMap<Long, Bid>();
    }

    public static void main(String [] args) {
        // get each consumer
        PJoin pjoin = new PJoin(new AuctionConsumer(), new BidConsumer());
        // do iterator model for consuming
        long procTuples = pjoin.startJoin();
        // keep both hashes in-memory
    }

    private long startJoin() {
        // start consumer thread
        this.tCon.start();
        // start prober thread
        this.tProbe.start();
        // start collector thread
        // one each for polling from consumers
        // an extra one for joining
        return joinState.size();
    }
}
