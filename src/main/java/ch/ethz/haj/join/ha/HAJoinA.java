package ch.ethz.haj.join.ha;

import ch.ethz.haj.KafkaConfig;
import ch.ethz.haj.consumer.AbstractConsumer;
import ch.ethz.haj.model.Auction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renatomarroquin on 2017-02-11.
 */
public class HAJoinA implements Callable<Map<Long, Set<String>>> {

    private final ConcurrentHashMap<Long, String> relA;
    private final AbstractConsumer relConsumer;
    private final ConcurrentHashMap<Long, Set<String>> relB;
    private final ConcurrentHashMap<Long, Long> objsDone;
    private final Logger logger;
    private HashMap<Long, Set<String>> matchTups;

    public HAJoinA(ConcurrentHashMap<Long, String> relA, ConcurrentHashMap<Long, Set<String>> relB,
                   AbstractConsumer relACon, ConcurrentHashMap<Long, Long> objsDone) {
        this.relA = relA;
        this.relB = relB;
        this.relConsumer = relACon;
        this.objsDone = objsDone;
        this.logger = LoggerFactory.getLogger(HAJoin.class);
    }

    public Map<Long, Set<String>> call() throws Exception {
        Vector<ConsumerRecord<Long, String>> records = relConsumer.nextBatch();
        this.matchTups = null;
        if (records != null) {
            this.matchTups = new HashMap<Long, Set<String>>();
            for (ConsumerRecord<Long, String> r : records) {
                Long recId = r.key();

                // check if done
                Auction tmpAuction = new Auction(r.value());
                if (tmpAuction.isDone()) {
                    objsDone.put(recId, tmpAuction.getTs());
                } else {
                    // check if it is a match
                    if (relB.containsKey(recId)) {
                        Set<String> bTuples = relB.get(recId);
                        // check if update, if it is
                        matchTups.put(recId, bTuples);
                    }
                    // add to relA
//                    if (!relA.containsKey(recId))
                        relA.put(recId, r.value());
                    // else // ignore because we only care
                }
            }
        }
        logger.debug(String.format("RelA contains %d records", relA.size()));
        return this.matchTups;
    }
}
