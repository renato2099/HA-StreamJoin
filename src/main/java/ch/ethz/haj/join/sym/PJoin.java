package ch.ethz.haj.join.sym;

import ch.ethz.haj.consumer.AbstractConsumer;
import ch.ethz.haj.consumer.AuctionConsumer;
import ch.ethz.haj.consumer.BidConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
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
    // Another option would be to have two threads.
    // 1 for reading A, and other for reading and probing B, and only keeping actual matching tuples
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
        //dumpJoinState(pjoin.joinState);
        // pjoin.terminateExecPool();
    }

    public static void dumpJoinState(ConcurrentHashMap<Long, Set<String>> joinState) {
        for (Map.Entry<Long, Set<String>> entry : joinState.entrySet()) {
            StringBuilder sb = new StringBuilder();
            Set<String> vals = entry.getValue();
            for (String v : vals)
                sb.append(v).append(" ");
            System.out.println(String.format("%d -> %s", entry.getKey(), sb.toString()));
        }
    }

    private void startJoin() throws Exception {
        int maxTries = 1000;
        int nTries = 0;
        SymHashJoinA joinA = new SymHashJoinA(relA, relB, relACon, objsDone);
        SymHashJoinB joinB = new SymHashJoinB(relA, relB, relBCon, objsDone);
        //while (numTries-- > 0) {
        int prevA = relA.size();
        int prevB = relB.size();
        int prevJs =joinState.size();

        while (nTries < maxTries) {
            // poll, probe,store from relA
            Map<Long, Set<String>> matchesRelA = joinA.call();
            updateJoinState(matchesRelA);
            // poll, probe, store from relB
            Map<Long, Set<String>> matchesRelB = joinB.call();
            updateJoinState(matchesRelB);
            // clean unneeded tuples from join state
            updateJoinState(objsDone);
            if (prevA == relA.size() && prevB == relB.size() && prevJs == joinState.size()) {
                nTries ++;
                if (nTries % 100 == 0)
                    logger.debug(String.format("RETRYING for the %dth time", nTries));
            }
            else
                logger.debug(String.format("RelA:%d\tRelB:%d\tJoinState:%d", relA.size(), relB.size(), joinState.size()));
            prevA = relA.size();
            prevB = relB.size();
            prevJs =joinState.size();
        }
        logger.info(String.format("RelA:%d\tRelB:%d\tJoinState:%d", relA.size(), relB.size(), joinState.size()));
    }

    private void updateJoinState(ConcurrentHashMap<Long, Long> objsDone) {
        Iterator<Map.Entry<Long, Long>> it = objsDone.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> entry = it.next();
            if (joinState.containsKey(entry.getKey())) {
                joinState.remove(entry.getKey());
                // we can just leave this as an increasing set so we can keep avoiding those tuples
                //objsDone.remove(entry.getKey());
            }
            if (relA.containsKey(entry.getKey())) {
                relA.remove(entry.getKey());
            }
            if (relB.containsKey(entry.getKey())) {
                relB.remove(entry.getKey());
            }
        }
    }

    private void updateJoinState(Map<Long, Set<String>> matches) {
        if (matches != null) {
            for (Map.Entry<Long, Set<String>> entry : matches.entrySet()) {
                joinState.put(entry.getKey(), entry.getValue());
            }
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
