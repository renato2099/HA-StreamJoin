package ch.ethz.haj.join.sym;

import ch.ethz.haj.consumer.AbstractConsumer;
import ch.ethz.haj.consumer.AuctionConsumer;
import ch.ethz.haj.consumer.BidConsumer;
import ch.ethz.haj.join.Join;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Created by marenato on 03.02.17.
 */
public class PJoin extends Join {

    public PJoin(AbstractConsumer auctionConsumer, AbstractConsumer bidConsumer) {
        super(auctionConsumer, bidConsumer);
        logger = LoggerFactory.getLogger(PJoin.class);
    }

    public static void main(String[] args) {
        parseOptions(args);
        PJoin pjoin = new PJoin(new AuctionConsumer(), new BidConsumer());
        try {
            pjoin.startJoin();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // pjoin.terminateExecPool();
    }

    @Override
    public void startJoin() throws Exception {
        long t0 = System.currentTimeMillis();
        int maxTries = 1000;
        int nTries = 0;
        SymHashJoinA joinA = new SymHashJoinA(relA, relB, relACon, objsDone);
        SymHashJoinB joinB = new SymHashJoinB(relA, relB, relBCon, objsDone);
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
        logger.info(String.format("[PJoin-%d] RelA:%d\tRelB:%d\tJoinState:k=%d tups=%d", missParts, getSzA(), getSzB(), joinState.size(), getJsSz()));
        long t1 = System.currentTimeMillis();
        logger.info(String.format("[PJoin-%d] Joining took: %s msecs", missParts, (t1-t0)));
        if (logger.isDebugEnabled())
            dumpJoinState(joinState);
    }
}
