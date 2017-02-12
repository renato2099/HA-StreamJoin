package ch.ethz.haj.join.sym;

import ch.ethz.haj.KafkaConfig;
import ch.ethz.haj.consumer.AbstractConsumer;
import ch.ethz.haj.model.Bid;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.spi.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by marenato on 06.02.17.
 */
public class SymHashJoinB implements Callable<Map<Long, Set<String>>> {
    private final ConcurrentHashMap<Long, Long> objsDone;
    private final ConcurrentHashMap<Long, String> relA;
    private final ConcurrentHashMap<Long, Set<String>> relB;
    private final AbstractConsumer relConsumer;
    private HashMap<Long, Set<String>> matchTups;
    private Logger logger;

    public SymHashJoinB(ConcurrentHashMap<Long, String> relA, ConcurrentHashMap<Long, Set<String>> relB,
                        AbstractConsumer relBCon, ConcurrentHashMap<Long, Long> objsDone) {
        this.relA = relA;
        this.relB = relB;
        this.relConsumer = relBCon;
        this.objsDone = objsDone;
        this.logger = org.slf4j.LoggerFactory.getLogger(SymHashJoinB.class);

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
                        Set<String> matchingTups = relB.get(b.getObjId());
                        if (matchingTups == null) {
                            matchingTups = new HashSet<String>();
                        }
                        matchingTups.add(r.value());
                        matchTups.put(b.getObjId(), matchingTups);
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
