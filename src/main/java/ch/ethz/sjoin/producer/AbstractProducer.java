package ch.ethz.sjoin.producer;

import ch.ethz.sjoin.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by marenato on 02.02.17.
 */
public class AbstractProducer extends KafkaConfig {

    // Kafka producer
    protected KafkaProducer<Long, String> producer;
    private volatile boolean running = true;

    /**
     * KafkaProducer constructor
     * @param kafkaTopic Kafka topic name
     */
    public AbstractProducer(String kafkaTopic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaUrl);
        properties.put("producer.type", "sync");
        properties.put("key.serializer", SERIALIZER);
        properties.put("value.serializer", SERIALIZER);
        this.kafkaTopic = kafkaTopic;
        // TODO this is not going to give us the best performance, change serializer
        this.producer = new KafkaProducer<Long, String>(properties);
    }

    /**
     * Sends messages into Kafka
     */
    protected void sendKafka(Integer partition, Long key, Long ts, String message) {
        this.producer.send(new ProducerRecord<Long, String>(this.kafkaTopic, partition, ts, key, message));
    }

    /**
     * Closes kafka producer
     */
    public void closeProducer() {
        this.producer.close();
    }

    /**
     * Wait for l seconds
     *
     * @param l
     */
    protected void politeWait(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException ex) {
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
