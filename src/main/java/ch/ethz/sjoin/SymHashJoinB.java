package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AbstractConsumer;
import ch.ethz.sjoin.model.Bid;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private final HashMap<Long, Set<String>> matchTups;

    public SymHashJoinB(ConcurrentHashMap<Long, String> relA, ConcurrentHashMap<Long, Set<String>> relB,
                        AbstractConsumer relBCon, ConcurrentHashMap<Long, Long> objsDone) {
        this.relA = relA;
        this.relB = relB;
        this.relConsumer = relBCon;
        this.objsDone = objsDone;
        this.matchTups = new HashMap<Long, Set<String>>();
    }

    public Map<Long, Set<String>> call() throws Exception {
        ConsumerRecords<Long, String> records = relConsumer.nextBatch();
        for (ConsumerRecord<Long, String> r: records) {
            Bid b = new Bid(r.value());
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
        return matchTups;
    }
}
