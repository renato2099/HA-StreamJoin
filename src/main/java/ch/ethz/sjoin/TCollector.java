package ch.ethz.sjoin;

import ch.ethz.sjoin.model.Bid;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by renatomarroquin on 2017-02-05.
 */
public class TCollector extends Thread {
    private final ConcurrentHashMap<Long, Set<Bid>> joinState;

    /**
     * Constructor
     * @param js join state
     */
    public TCollector(ConcurrentHashMap<Long, Set<Bid>> js) {
        this.joinState = js;
    }

    /**
     * Delete object id from current join state
     * @param objId
     * @return
     */
    public boolean remove(Long objId) {
        Set<Bid> removed = this.joinState.remove(objId);
        boolean result = false;
        if (removed != null)
            result = true;
        return result;
    }
}
