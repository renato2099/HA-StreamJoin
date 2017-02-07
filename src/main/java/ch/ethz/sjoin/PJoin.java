package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AbstractConsumer;
import ch.ethz.sjoin.consumer.AuctionConsumer;
import ch.ethz.sjoin.consumer.BidConsumer;
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
    private final ConcurrentHashMap<Long, Long> objsDone;
    private AbstractConsumer relACon;
    private AbstractConsumer relBCon;
    private ConcurrentHashMap<Long, Set<String>> joinState;
    private ConcurrentHashMap<Long, String> relA;
    private ConcurrentHashMap<Long, Set<String>> relB;
    private Logger logger;
    private ExecutorService execs;

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

    public static void main(String[] args) {
        PJoin pjoin = new PJoin(new AuctionConsumer(), new BidConsumer());
        try {
            pjoin.startJoin();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        pjoin.terminateExecPool();
    }

    private void startJoin() throws Exception {
        int numTries = 20;
        SymHashJoinA joinA = new SymHashJoinA(relA, relB, relACon, objsDone);
        SymHashJoinB joinB = new SymHashJoinB(relA, relB, relBCon, objsDone);
        while (numTries-- > 0) {
            // poll, probe,store from relA
            //Future<Map<Long, Set<String>>> matchesRelA = execs.submit(new SymHashJoinA(relA, relB, relACon, objsDone));
            Map<Long, Set<String>> matchesRelA = joinA.call();
            updateJoinState(matchesRelA);
            // poll, probe, store from relB
            //Future<Map<Long, Set<String>>> matchesRelB = execs.submit(new SymHashJoinB(relA, relB, relBCon, objsDone));
            Map<Long, Set<String>> matchesRelB = joinB.call();
            updateJoinState(matchesRelB);
            // clean unneeded tuples from join state
//            updateJoinState(objsDone);
            logger.info(String.format("RelA:%d\tRelB:%d\tJoinState:%d", relA.size(), relB.size(), joinState.size()));
        }
    }

    private void updateJoinState(ConcurrentHashMap<Long, Long> objsDone) {
        for (Map.Entry<Long, Long> entry : objsDone.entrySet()) {
            if (joinState.contains(entry.getKey()))
                joinState.remove(entry.getKey());
        }
    }

    private void updateJoinState(Map<Long, Set<String>> matches) {
        for (Map.Entry<Long, Set<String>> entry : matches.entrySet()) {
            joinState.put(entry.getKey(), entry.getValue());
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
