package ch.ethz.sjoin;

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
    public static final String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    // Kafka url
    public static String kafkaUrl = DEF_KAFKA_URL;
    // Zk url
    public static String zkUrl = DEF_ZK_URL;
    // missing partitions
    public static int missParts = 0;
    // scaling factor
    public static int sf = 1;
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
            KafkaOpts prodOpts = KafkaOpts.valueOf(optParts[0].toLowerCase());

            if (prodOpts != null) {
                String val = optParts[1];
                switch (prodOpts) {
                    case KAFKA_URL:
                        kafkaUrl = val;
                        break;
                    case ZK:
                        zkUrl = val;
                        break;
                    case MISSING_PARTITIONS:
                        missParts = Integer.parseInt(val);
                        break;
                    case SF:
                        sf = Integer.parseInt(val);
                        break;
                }
            }
        }
    }

    public enum KafkaOpts {
        KAFKA_URL("kafka"), ZK("zk"), MISSING_PARTITIONS("missing"), SF("sf");;
        String value;

        KafkaOpts(String v) {
            this.value = v;
        }
    }
}
