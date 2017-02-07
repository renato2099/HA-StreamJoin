package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AbstractConsumer;
import ch.ethz.sjoin.model.Auction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by marenato on 06.02.17.
 */
public class SymHashJoinA implements Callable<Map<Long, Set<String>>> {
    private final ConcurrentHashMap<Long, String> relA;
    private final AbstractConsumer relConsumer;
    private final ConcurrentHashMap<Long, Set<String>> relB;
    private final HashMap<Long, Set<String>> matchTups;
    private final ConcurrentHashMap<Long, Long> objsDone;
    private final Logger logger;

    public SymHashJoinA(ConcurrentHashMap<Long, String> relA, ConcurrentHashMap<Long, Set<String>> relB,
                        AbstractConsumer rc, ConcurrentHashMap<Long, Long> objsDone) {
        this.relA = relA;
        this.relB = relB;
        this.relConsumer = rc;
        this.matchTups = new HashMap<Long, Set<String>>();
        this.objsDone = objsDone;
        this.logger = LoggerFactory.getLogger(SymHashJoinA.class);
    }

    public Map<Long, Set<String>> call() throws Exception {
        Map<Long, String> records = relConsumer.nextBatch();
        for (Map.Entry<Long, String> r : records.entrySet()) {
            Long recId = r.getKey();
            // check if it is a match
            if (relB.containsKey(recId)) {
                Set<String> bTuples = relB.get(recId);
                if (!matchTups.containsKey(recId)) {
                    // sanity check
                    if (bTuples == null)
                        throw new IllegalStateException(String.format("BID: auction %d without bids", recId));
                    // If I haven't seen it before, then put it as result
                    matchTups.put(recId, bTuples);
                } else {
                    // if I have seen it before, then check if it has to be deleted
                    Auction tmpAuction = new Auction(r.getValue());
                    if (tmpAuction.getInfo() == null) {
                        objsDone.put(recId, tmpAuction.getTs());
                    }
                    // or it might be an update, which in this case we don't case
                }
            }
            // add to relA
            relA.put(recId, r.getValue());
        }
        logger.debug(String.format("RelA contains %d records", relA.size()));
        return this.matchTups;
    }
}
