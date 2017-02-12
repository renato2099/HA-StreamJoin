package ch.ethz.haj.join.ha;

import ch.ethz.haj.consumer.AbstractConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
        return null;
    }
}
