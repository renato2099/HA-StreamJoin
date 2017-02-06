package ch.ethz.sjoin;

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
    // Kafka serializer/deserializer classes
    public static final String KEY_SERIALIZER = LongSerializer.class.getCanonicalName();
    public static final String KEY_DESERIALIZER = LongDeserializer.class.getCanonicalName();
    public static final String VAL_SERIALIZER = StringSerializer.class.getCanonicalName();
    public static final String VAL_DESERIALIZER = StringDeserializer.class.getCanonicalName();
    // Kafka url
    public static String kafkaUrl = DEF_KAFKA_URL;
    // Zk url
    public static String zkUrl = DEF_ZK_URL;
    // missing partitions
    public static int missParts = 0;
    // scaling factor
    public static int sf = 1;
    public static long tuplesSf = TUPLES_SF;
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
                }
            }
        }
    }

    public enum KafkaOpts {
        KAFKA("kafka"), ZK("zk"), MISSING("missing"), SF("sf"), TUPLES("tuples");
        String value;

        KafkaOpts(String v) {
            this.value = v;
        }
    }
}
