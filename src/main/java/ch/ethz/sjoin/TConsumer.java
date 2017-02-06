package ch.ethz.sjoin;

import ch.ethz.sjoin.consumer.AbstractConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renatomarroquin on 2017-02-05.
 */
public class TConsumer extends Thread {
    // waiting some time for new records
    private static final long WAIT_TIME = 5000;
    // relation consumer
    private final AbstractConsumer relCon;
    // hash-map containing ids from relation A
    private final ConcurrentHashMap<Long, Long> relA;
    // thread logger
    private static Logger logger = LoggerFactory.getLogger(TConsumer.class);

    /**
     * Constructor
     * @param aConsumer Relation consumer for kafka topic
     * @param relA HashMap for containing relation fetched data
     */
    public TConsumer(AbstractConsumer aConsumer, ConcurrentHashMap<Long, Long> relA) {
        this.relCon = aConsumer;
        this.relA = relA;
    }

    public void run() {
        logger.info(String.format("Starting to consume tuples from: %s", this.relCon.kafkaTopic));
        while (true) {
            ConsumerRecords<Long, String> relRecords = this.relCon.nextBatch();
            if (relRecords.count() > 0) {
                for (ConsumerRecord<Long, String> r : relRecords) {
                    relA.putIfAbsent(r.key(), r.timestamp());
                }
            }
            else {
                this.politeWait(WAIT_TIME);
            }
        }
    }

    /**
     * Wait for l seconds
     *
     * @param l
     */
    protected void politeWait(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
