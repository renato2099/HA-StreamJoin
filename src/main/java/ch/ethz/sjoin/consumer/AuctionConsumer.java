
package ch.ethz.sjoin.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.DocFlavor;

/**
 * Created by marenato on 03.02.17.
 */
public class AuctionConsumer extends AbstractConsumer {
    // consumer group id
    private static String DEF_GROUP_ID = "AuctionConsumerGroup";

    /**
     * Default constructor
     */
    public AuctionConsumer() {
        super(AUCTION_TOPIC, DEF_GROUP_ID);
        this.logger = LoggerFactory.getLogger(AuctionConsumer.class);
    }

    public static void main(String [] args) {

    }
}
