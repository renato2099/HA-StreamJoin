package ch.ethz.sj;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by marenato on 02.02.17.
 */
public class AbstractProducer {
    public enum ProducerOpts {
        KAFKA_URL("kafka"), ZK("zk");
        String value;
        ProducerOpts(String v) {
            this.value = v;
        }
    }

    /**
     * Kafka url
     */
    public static final String DEF_KAFKA_URL = "localhost:9092";
    public static final String DEF_ZK_URL = "localhost:2181";

    public static String kafkaUrl = DEF_KAFKA_URL;
    public static String zkUrl = DEF_ZK_URL;

    /**
     * Kafka serializer class
     */
    public static final String SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * Kafka producer
     */
    protected KafkaProducer<String, String> producer;
    private volatile boolean running = true;
    private String kafkaTopic;

    /**
     * Kafka producer default initializer
     */
    protected void initializeKafkaProducer(String kafkaTopic, String kafkaUrl, String zkUrl) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaUrl);
        properties.put("producer.type", "sync");
        properties.put("key.serializer", SERIALIZER);
        properties.put("value.serializer", SERIALIZER);
        this.kafkaTopic = kafkaTopic;
        this.zkUrl = zkUrl;
        // TODO this is not going to give us the best performance, change serializer
        this.producer = new KafkaProducer<String, String>(properties);
    }

    /**
     * Sends messages into Kafka
     */
    protected void sendKafka(Integer partition, String key, Long ts, String message) {
        this.producer.send(new ProducerRecord<String, String>(this.kafkaTopic, partition, ts, key, message));
    }

    /**
     * Closes kafka producer
     */
    public void closeProducer() {
        this.producer.close();
    }

    /**
     * Wait for l seconds
     * @param l
     */
    protected void politeWait(long l) {
        try {
            Thread.sleep(l);
        } catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
