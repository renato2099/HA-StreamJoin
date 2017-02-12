package ch.ethz.haj.join;

import ch.ethz.haj.KafkaConfig;
import ch.ethz.haj.consumer.AbstractConsumer;
import org.slf4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by renatomarroquin on 2017-02-11.
 */
public class Join extends KafkaConfig {
    public static final int THREAD_POOL = 2;
    public static final long WAIT_TERM = 1000;
    public AbstractConsumer relACon;
    public AbstractConsumer relBCon;
    public final ConcurrentHashMap<Long, Set<String>> joinState;
    public final ConcurrentHashMap<Long, String> relA;
    public final ConcurrentHashMap<Long, Set<String>> relB;
    // auctionedObjs once they expired
    public final ConcurrentHashMap<Long, Long> objsDone;

    public Logger logger;
    // Another option would be to have two threads.
    // 1 for reading A, and other for reading and probing B, and only keeping actual matching tuples
    public ExecutorService execs;

    public Join(AbstractConsumer aConsumer, AbstractConsumer bConsumer) {
        relACon = aConsumer;
        relBCon = bConsumer;
        joinState = new ConcurrentHashMap<Long, Set<String>>();
        relA = new ConcurrentHashMap<Long, String>();
        relB = new ConcurrentHashMap<Long, Set<String>>();
        execs = Executors.newFixedThreadPool(THREAD_POOL);
        objsDone = new ConcurrentHashMap<Long, Long>();
    }

    public void startJoin() throws Exception {
    }

    protected void updateJoinState(Map<Long, Set<String>> matches) {
        if (matches != null) {
            for (Map.Entry<Long, Set<String>> entry : matches.entrySet()) {
                joinState.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public long getJoinStateTuples() {
        long nTuples = 0;
        for (Map.Entry<Long, Set<String>> entry : joinState.entrySet()) {
            nTuples += entry.getValue().size();
        }
        return nTuples;
    }

    public void dumpJoinState(ConcurrentHashMap<Long, Set<String>> joinState) {
        for (Map.Entry<Long, Set<String>> entry : joinState.entrySet()) {
            StringBuilder sb = new StringBuilder();
            Set<String> vals = entry.getValue();
            for (String v : vals)
                sb.append(v).append(" ");
            System.out.println(String.format("%d -> %s", entry.getKey(), sb.toString()));
        }
    }

    public void terminateExecPool() {
        try {
            logger.info("Terminating executors pool.");
            execs.awaitTermination(WAIT_TERM, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
