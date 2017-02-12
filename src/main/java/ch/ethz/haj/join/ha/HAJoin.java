package ch.ethz.haj.join.ha;

import ch.ethz.haj.consumer.AuctionConsumer;
import ch.ethz.haj.consumer.BidConsumer;
import ch.ethz.haj.join.Join;
import ch.ethz.haj.join.sym.SymHashJoinA;
import ch.ethz.haj.join.sym.SymHashJoinB;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by marenato on 08.02.17.
 */
public class HAJoin extends Join {

    public HAJoin(AuctionConsumer auctionConsumer, BidConsumer bidConsumer) {
        super(auctionConsumer, bidConsumer);
        relACon = auctionConsumer;
        relBCon = bidConsumer;
        logger = LoggerFactory.getLogger(HAJoin.class);
        // relations should contain key+ts or just deserialize them
    }

    public static void main(String[] args) {
        HAJoin hajoin = new HAJoin(new AuctionConsumer(), new BidConsumer());
        try {
            hajoin.startJoin();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void startJoin() throws Exception {
        long t0 = System.currentTimeMillis();
        int maxTries = 1000;
        int nTries = 0;
        HAJoinA joinA = new HAJoinA(relA, relB, relACon, objsDone);
        HAJoinB joinB = new HAJoinB(relA, relB, relBCon, objsDone);
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
                    logger.info(String.format("RETRYING for the %dth time", nTries));
            }
            else
                logger.debug(String.format("RelA:%d\tRelB:%d\tJoinState:%d", relA.size(), relB.size(), joinState.size()));
            prevA = relA.size();
            prevB = relB.size();
            prevJs =joinState.size();
        }

        logger.info(String.format("RelA:%d\tRelB:%d\tJoinState:k=%d tups=%d", relA.size(), relB.size(), joinState.size(), getJoinStateTuples()));
        long t1 = System.currentTimeMillis();
        logger.info(String.format("Joining took: %s msecs", (t1-t0)));
        if (logger.isDebugEnabled()) {
            System.out.println("====================== JS ======================");
            dumpJoinState(joinState);
//            System.out.println("====================== RelA ======================");
//            dumpRelation(relA);
//            System.out.println("====================== RelB ======================");
//            dumpJoinState(relB);
        }
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
}
