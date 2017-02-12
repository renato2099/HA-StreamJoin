package ch.ethz.haj.join.ha;

import ch.ethz.haj.consumer.AuctionConsumer;
import ch.ethz.haj.consumer.BidConsumer;
import ch.ethz.haj.join.Join;
import ch.ethz.haj.join.sym.SymHashJoinA;
import ch.ethz.haj.join.sym.SymHashJoinB;
import org.slf4j.LoggerFactory;

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
        HAJoinA joinA = new HAJoinA(relA, relB, relACon, objsDone);
        HAJoinB joinB = new HAJoinB(relA, relB, relBCon, objsDone);



        logger.info(String.format("RelA:%d\tRelB:%d\tJoinState:%d", relA.size(), relB.size(), joinState.size()));
        long t1 = System.currentTimeMillis();
        logger.info(String.format("Joining took: %s msecs", (t1-t0)));
        if (logger.isDebugEnabled())
            dumpJoinState(joinState);

    }
}
