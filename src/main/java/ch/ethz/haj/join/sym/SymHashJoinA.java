package ch.ethz.haj.join.sym;

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
 * Created by marenato on 06.02.17.
 */
public class SymHashJoinA implements Callable<Map<Long, Set<String>>> {
    private final ConcurrentHashMap<Long, String> relA;
    private final AbstractConsumer relConsumer;
    private final ConcurrentHashMap<Long, Set<String>> relB;
    private final ConcurrentHashMap<Long, Long> objsDone;
    private final Logger logger;
    private HashMap<Long, Set<String>> matchTups;

    public SymHashJoinA(ConcurrentHashMap<Long, String> relA, ConcurrentHashMap<Long, Set<String>> relB,
                        AbstractConsumer rc, ConcurrentHashMap<Long, Long> objsDone) {
        this.relA = relA;
        this.relB = relB;
        this.relConsumer = rc;
        this.objsDone = objsDone;
        this.logger = LoggerFactory.getLogger(SymHashJoinA.class);
    }

    public Map<Long, Set<String>> call() throws Exception {
        Vector<ConsumerRecord<Long, String>> records = relConsumer.nextBatch();
        this.matchTups = null;
        if (records != null) {
            this.matchTups = new HashMap<Long, Set<String>>();
            for (ConsumerRecord<Long, String> r : records) {
                Long recId = r.key();
                // skipping <lost> partitions as we can't do any guarantees on them
                if (recId%KafkaConfig.NUM_PARTS < KafkaConfig.missParts)
                    continue;

                // check if done
                Auction tmpAuction = new Auction(r.value());
                if (tmpAuction.isDone()) {
                    objsDone.put(recId, tmpAuction.getTs());
                } else {
                    // check if it is a match
                    if (relB.containsKey(recId)) {
                        Set<String> bTuples = relB.get(recId);
                        // check if update
                        if (!matchTups.containsKey(recId)) {
                            // sanity check
                            if (bTuples == null)
                                throw new IllegalStateException(String.format("BID: auction %d without bids", recId));
                            // If I haven't seen it before, then put it as result
                            matchTups.put(recId, bTuples);
                        }
                        // ignore updates
                    }
                    // add to relA
                    relA.put(recId, r.value());
                }
            }
        }
        logger.debug(String.format("RelA contains %d records", relA.size()));
        return this.matchTups;
    }
}
