import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

public class ProductorTweets {

    public final static String TOPIC_NAME = "rawtweets";

    public static void main(String[] args) {

        String apiKey = args[0];
        String apiSecret = args[1];
        String tokenValue = args[2];
        String tokenSecret = args[3];

        Properties properties = new Properties();
        properties.put("acks", "1");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuth2AccessToken(tokenValue);
        configurationBuilder.setOAuthAccessTokenSecret(tokenSecret);
        configurationBuilder.setOAuthConsumerKey(apiKey);
        configurationBuilder.setOAuthConsumerSecret(apiSecret);
        configurationBuilder.setJSONStoreEnabled(true);
        configurationBuilder.setIncludeEntitiesEnabled(true);

        final TwitterStream twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();

        try {
            StatusListener listenerEx = new StatusListener() {
                @Override
                public void onStatus(Status status) {
                    HashtagEntity[] hashtags = status.getHashtagEntities();
                    if (hashtags.length > 0) {
                        String value = TwitterObjectFactory.getRawJSON(status);
                        String lang = status.getLang();
                        producer.send(new ProducerRecord<>(ProductorTweets.TOPIC_NAME, lang, value));
                    }
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) { }

                @Override
                public void onTrackLimitationNotice(int i) { }

                @Override
                public void onScrubGeo(long l, long l1) { }

                @Override
                public void onStallWarning(StallWarning stallWarning) { }

                @Override
                public void onException(Exception e) { }
            };
            twitterStream.addListener(listenerEx);
            twitterStream.sample();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
