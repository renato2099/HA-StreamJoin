package ch.ethz.haj;

import org.apache.kafka.common.serialization.*;

/**
 * Created by marenato on 03.02.17.
 */
public abstract class KafkaConfig {
    // BidTopic name
    public static final String BID_TOPIC = "bid-topic";
    // AuctionTopic name
    public static final String AUCTION_TOPIC = "auction-topic";

    public static final String DEF_KAFKA_URL = "localhost:9092";
    public static final String DEF_ZK_URL = "localhost:2181";
    public static final long TUPLES_SF = 100;
    public static final int BID_RATIO = 10;
    public static final double PCOMPLETION = 0.5;
    // Porcentage of tuples produced until failures start happening
    public static final double SUCCESS_TUPS = 1.0;
    // Kafka serializer/deserializer classes
    public static final String KEY_SERIALIZER = LongSerializer.class.getCanonicalName();
    public static final String KEY_DESERIALIZER = LongDeserializer.class.getCanonicalName();
    public static final String VAL_SERIALIZER = StringSerializer.class.getCanonicalName();
    public static final String VAL_DESERIALIZER = StringDeserializer.class.getCanonicalName();
    // Kafka url
    public static String kafkaUrl = DEF_KAFKA_URL;
    // Zk url
    public static String zkUrl = DEF_ZK_URL;
    // Number of partitions failing
    public static int missParts = 0;
    // scaling factor
    public static int sf = 1;
    public static long tuplesSf = TUPLES_SF;
    public static int bidRatio = BID_RATIO;
    public static double pCompletion = PCOMPLETION;
    public static double pSuccess = SUCCESS_TUPS;
    // kafka topic
    public String kafkaTopic;
    // kafka consumer groupId
    public String kafkaGroupId;

    /**
     * Parse cmd line arguments to be used by consumers/producers.
     *
     * @param args
     */
    public static void parseOptions(String[] args) {
        for (String opt : args) {
            String[] optParts = opt.split("=");
            KafkaOpts prodOpts = KafkaOpts.valueOf(optParts[0].toUpperCase());

            if (prodOpts != null) {
                String val = optParts[1];
                switch (prodOpts) {
                    case KAFKA:
                        kafkaUrl = val;
                        break;
                    case ZK:
                        zkUrl = val;
                        break;
                    case MISSING:
                        missParts = Integer.parseInt(val);
                        break;
                    case SF:
                        sf = Integer.parseInt(val);
                        break;
                    case TUPLES:
                        tuplesSf = Long.parseLong(val);
                        break;
                    case BID_RATIO:
                        bidRatio = Integer.parseInt(val);
                        break;
                    case PCOMPLETION:
                        pCompletion = Double.parseDouble(val);
                        break;
                    case PSUCCESS:
                        pSuccess = Double.parseDouble(val);
                        break;
                }
            }
        }
    }

    public enum KafkaOpts {
        KAFKA("kafka"), ZK("zk"), MISSING("missing"), SF("sf"), TUPLES("tuples"),
        BID_RATIO("bid_ratio"), PCOMPLETION("pcompletion"), PSUCCESS("psuccess");
        String value;

        KafkaOpts(String v) {
            this.value = v;
        }
    }
}
