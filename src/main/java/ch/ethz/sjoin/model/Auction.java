package ch.ethz.sjoin.model;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Created by marenato on 02.02.17.
 */
public class Auction {

    private long id;
    private String info;
    private long ts;

    public Auction(long id, String info, long ts) {
        this.id = id;
        this.info = info;
        this.ts = ts;
    }

    public Auction(long id) {
        this.id = id;
    }

    public Auction(String jsonStr) {
        JSONObject jObj;
        long tmpId = -1;
        String tmpInfo = "";
        long tmpTs = -1;
        try {
            jObj = (JSONObject) new JSONParser().parse(jsonStr);
            tmpId = Long.valueOf(jObj.get("id").toString());
            tmpInfo = jObj.get("info").toString();
            tmpTs = Long.valueOf(jObj.get("ts").toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        this.id = tmpId;
        this.info = tmpInfo;
        this.ts = tmpTs;
    }

    public void setInfo(String i) {
        this.info = i;
    }

    public void setTs(long t) {
        this.ts = t;
    }

    public String toJson() {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("id", id);
        jsonObj.put("info", info);
        jsonObj.put("ts", ts);
        return jsonObj.toJSONString();
    }

    public long getTs() {
        return ts;
    }

    public long getId() {
        return id;
    }

    public String getInfo() {
        return info;
    }

    public String getStrId() {
        return String.valueOf(id);
    }
}
