package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AbstractConsumer;
import ch.ethz.sjoin.model.Bid;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renatomarroquin on 2017-02-05.
 */
public class TProber extends Thread {
    private final AbstractConsumer relCon;
    private final ConcurrentHashMap<Long, Long> relA;
    private final ConcurrentHashMap<Long, Bid> relB;
    private final ConcurrentHashMap<Long, Set<Bid>> joinState;
    // set of tuples that didn't match, but might in the future
    private Set<Bid> kept;
    // thread logger
    private static Logger logger = LoggerFactory.getLogger(TProber.class);

    /**
     * Constructor
     *
     * @param bConsumer KafkaConsumer for relation B
     * @param relB      Relation B fetched state
     */
    public TProber(AbstractConsumer bConsumer, ConcurrentHashMap<Long, Long> relA, ConcurrentHashMap<Long, Bid> relB,
                   ConcurrentHashMap<Long, Set<Bid>> js) {
        this.relCon = bConsumer;
        this.relA = relA;
        this.relB = relB;
        this.joinState = js;
        this.kept = new HashSet<Bid>();
    }

    public void run() {
        logger.info(String.format("Starting to consume tuples from: %s", this.relCon.kafkaTopic));
        while (true) {
            ConsumerRecords<Long, String> records = this.relCon.nextBatch();
            // received tuples
            for (ConsumerRecord<Long, String> r : records) {
                Bid tmpBid = new Bid(r.value());
                // if we don't find its object id then keep it
                if (!probe(tmpBid)) {
                    this.kept.add(tmpBid);
                }
            }
            // check kept against new arrivals
            for (Iterator<Bid> bi = this.kept.iterator(); bi.hasNext();) {
                Bid b = bi.next();
                if (probe(b)) {
                    //delete
                    bi.remove();
                }
            }
        }
    }

    public boolean probe(Bid b) {
        boolean probeRes = false;
        if (this.relA.contains(b.getObjId())) {
            // if it matches, then add it to js
            Set<Bid> bids = this.joinState.get(b.getObjId());
            if (bids == null) {
                bids = new HashSet<Bid>();
            }
            bids.add(b);
            this.joinState.put(b.getObjId(), bids);
            probeRes = true;
        }
        return probeRes;
    }
}
