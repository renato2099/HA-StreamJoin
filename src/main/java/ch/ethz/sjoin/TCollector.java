package ch.ethz.sjoin;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renatomarroquin on 2017-02-05.
 */
public class TCollector extends Thread {
    private final ConcurrentHashMap<Long, Set<Long>> joinState;

    /**
     * Constructor
     * @param js join state
     */
    public TCollector(ConcurrentHashMap<Long, Set<Long>> js) {
        this.joinState = js;
    }

    /**
     * Delete object id from current join state
     * @param objId
     * @return
     */
    public boolean remove(Long objId) {
        Set<Long> removed = this.joinState.remove(objId);
        boolean result = false;
        if (removed != null)
            result = true;
        return result;
    }
}
