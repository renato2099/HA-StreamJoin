package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AbstractConsumer;
import ch.ethz.sjoin.consumer.AuctionConsumer;
import ch.ethz.sjoin.consumer.BidConsumer;
import ch.ethz.sjoin.model.Bid;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by marenato on 03.02.17.
 */
public class PJoin {

    private static final int THREAD_POOL = 2;
    private static final long WAIT_TERM = 1000;
    private AbstractConsumer relACon;
    private AbstractConsumer relBCon;
    private ConcurrentHashMap<Long, Set<String>> joinState;
    private ConcurrentHashMap<Long, String> relA;
    private ConcurrentHashMap<Long, Set<String>> relB;
    private Logger logger;
    private ExecutorService execs;
    private final ConcurrentHashMap<Long, Long> objsDone;

    public PJoin(AbstractConsumer auctionConsumer, AbstractConsumer bidConsumer) {
        relACon = auctionConsumer;
        relBCon = bidConsumer;
        joinState = new ConcurrentHashMap<Long, Set<String>>();
        relA = new ConcurrentHashMap<Long, String>();
        relB = new ConcurrentHashMap<Long, Set<String>>();
        logger = LoggerFactory.getLogger(PJoin.class);
        execs = Executors.newFixedThreadPool(THREAD_POOL);
        objsDone = new ConcurrentHashMap<Long, Long>();
    }

    public static void main(String [] args) {
        // get each consumer
//        BidConsumer bidCon = new BidConsumer();
//        while(true) {
//            ConsumerRecords<Long, String> recs = bidCon.nextBatch();
//            bidCon.printRecords(recs);
//        }

        PJoin pjoin = new PJoin(new AuctionConsumer(), new BidConsumer());
        // do iterator model for consuming
        pjoin.startJoin();
//        pjoin.terminateExecPool();
    }

    private void startJoin() {
        int numTries = 20;
        while (numTries --> 0) {
            // poll, probe,store from relA
//            Future<Map<Long, Set<String>>> matchesRelA = execs.submit(new SymHashJoinA(relA, relB, relACon, objsDone));
            // poll, probe, store from relB
            Future<Map<Long, Set<String>>> matchesRelB = execs.submit(new SymHashJoinB(relA, relB, relBCon, objsDone));
            // clean unneeded tuples from join state
//            updateJoinState(matchesRelA);
            updateJoinState(matchesRelB);
//            updateJoinState(objsDone);
            logger.info(String.format("RelA:%d\tRelB:%d\tJoinState:%d", relA.size(), relB.size(), joinState.size()));
        }
    }

    private void updateJoinState(ConcurrentHashMap<Long, Long> objsDone) {
        for (Map.Entry<Long, Long> entry: objsDone.entrySet()) {
            if (joinState.contains(entry.getKey()))
                joinState.remove(entry.getKey());
        }
    }

    private void updateJoinState(Future<Map<Long, Set<String>>> futMatches) {
        try {
            Map<Long, Set<String>> matches = futMatches.get();
            for (Map.Entry<Long, Set<String>> entry : matches.entrySet()) {
                joinState.put(entry.getKey(), entry.getValue());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void terminateExecPool() {
        try {
            logger.info("Terminating executors pool.");
            execs.awaitTermination(WAIT_TERM, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
