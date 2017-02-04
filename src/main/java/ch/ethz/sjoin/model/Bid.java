package ch.ethz.sjoin.model;

import org.json.simple.JSONObject;

/**
 * Created by marenato on 02.02.17.
 */
public class Bid {
    private final long id;
    private final long objId;
    private final double price;
    private final long ts;

    public Bid(long id, long objectId, double price, long ts) {
        this.id = id;
        this.objId = objectId;
        this.price = price;
        this.ts = ts;
    }

    public String toJson(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", id);
        jsonObject.put("objId", objId);
        jsonObject.put("price", price);
        jsonObject.put("ts", ts);
        return jsonObject.toJSONString();
    }

    public long getId() {
        return id;
    }

    public String getStrId() {
        return String.valueOf(id);
    }

    public long getObjId() {
        return objId;
    }

    public long getTs() {
        return ts;
    }

    public String getStrObjId() {
        return String.valueOf(objId);
    }
}
