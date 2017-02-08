package ch.ethz.haj.model;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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

    public Bid(String jsonStr) {
        JSONObject jObj;
        long tmpId = -1;
        long tmpObjId = -1;
        double tmpPrice = -1.0;
        long tmpTs = -1;
        try {
            jObj = (JSONObject) new JSONParser().parse(jsonStr);
            tmpId = Long.valueOf(jObj.get("id").toString());
            tmpObjId = Long.valueOf(jObj.get("objId").toString());
            tmpPrice = Double.valueOf(jObj.get("price").toString());
            tmpTs = Long.valueOf(jObj.get("ts").toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        this.id = tmpId;
        this.objId = tmpObjId;
        this.price = tmpPrice;
        this.ts = tmpTs;
    }

    public String toJson(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", id);
        jsonObject.put("objId", objId);
        jsonObject.put("price", price);
        jsonObject.put("ts", ts);
        return jsonObject.toJSONString();
    }

    public Long getId() {
        return id;
    }

    public String getStrId() {
        return String.valueOf(id);
    }

    public Long getObjId() {
        return objId;
    }

    public long getTs() {
        return ts;
    }

    public String getStrObjId() {
        return String.valueOf(objId);
    }
}
