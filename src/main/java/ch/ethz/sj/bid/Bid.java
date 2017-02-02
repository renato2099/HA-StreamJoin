package ch.ethz.sj.bid;

import org.json.simple.JSONObject;

/**
 * Created by marenato on 02.02.17.
 */
public class Bid {
    private final long objId;
    private final double price;
    private final long ts;

    public Bid(long objectId, double price, long ts) {
        this.objId = objectId;
        this.price = price;
        this.ts = ts;
    }

    public String toJson(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("objId", objId);
        jsonObject.put("price", price);
        jsonObject.put("ts", ts);
        return jsonObject.toJSONString();
    }
}
