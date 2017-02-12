package ch.ethz.haj.join.ha;

import ch.ethz.haj.consumer.AbstractConsumer;
import ch.ethz.haj.model.Auction;
import ch.ethz.haj.model.Bid;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renatomarroquin on 2017-02-11.
 */
public class HAJoinB implements Callable<Map<Long, Set<String>>> {

    private final ConcurrentHashMap<Long, String> relA;
    private final AbstractConsumer relConsumer;
    private final ConcurrentHashMap<Long, Set<String>> relB;
    private final ConcurrentHashMap<Long, Long> objsDone;
    private final Logger logger;
    private HashMap<Long, Set<String>> matchTups;

    public HAJoinB(ConcurrentHashMap<Long, String> relA, ConcurrentHashMap<Long, Set<String>> relB,
                   AbstractConsumer relBCon, ConcurrentHashMap<Long, Long> objsDone) {
        this.relA = relA;
        this.relB = relB;
        this.relConsumer = relBCon;
        this.objsDone = objsDone;
        this.logger = LoggerFactory.getLogger(HAJoin.class);
    }

    public Map<Long, Set<String>> call() throws Exception {
        Vector<ConsumerRecord<Long, String>> records = relConsumer.nextBatch();
        this.matchTups = null;
        if (records != null) {
            this.matchTups = new HashMap<Long, Set<String>>();
            for (ConsumerRecord<Long, String> r: records) {
                Bid b = new Bid(r.value());

                // if it's an object done, then ignore
                if (!objsDone.containsKey(b.getObjId())) {
                    // check if it's a match
                    if (relA.containsKey(b.getObjId())) {
                        Auction rAuction = new Auction(relA.get(b.getObjId()));

//                        if (b.getTs() < rAuction.getTs()) {
                            Set<String> matchingTups = relB.get(b.getObjId());
                            if (matchingTups == null) {
                                matchingTups = new HashSet<String>();
                            }
                            matchingTups.add(r.value());
                            matchTups.put(b.getObjId(), matchingTups);
//                        }
                    }
                    // add to relB
                    Set<String> bids = relB.get(b.getObjId());
                    if (bids == null) {
                        bids = new HashSet<String>();
                    }
                    bids.add(r.value());
                    relB.put(b.getObjId(), bids);
                }
            }
        }
        logger.debug(String.format("RelB contains %d records", relB.size()));
        return matchTups;
    }
}
