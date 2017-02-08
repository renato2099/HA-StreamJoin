package ch.ethz.haj.join.ha;

import ch.ethz.haj.consumer.AuctionConsumer;
import ch.ethz.haj.consumer.BidConsumer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static ch.ethz.haj.join.sym.PJoin.dumpJoinState;

/**
 * Created by marenato on 08.02.17.
 */
public class HAJoin {
    private final AuctionConsumer relACon;
    private final BidConsumer relBCon;
    private ConcurrentHashMap<Long, Set<String>> joinState;

    public HAJoin(AuctionConsumer auctionConsumer, BidConsumer bidConsumer) {
        relACon = auctionConsumer;
        relBCon = bidConsumer;
        // relations should contain key+ts or just deserialize them
    }

    public static void main(String [] args) {
        HAJoin hajoin = new HAJoin(new AuctionConsumer(), new BidConsumer());
        try {
            hajoin.startJoin();
        } catch (Exception e) {
            e.printStackTrace();
        }
        dumpJoinState(hajoin.joinState);
    }

    private void startJoin() {

    }
}
