import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

import java.util.Collections;
import java.util.Properties;

public class TweetsHashtagsCounter {

    public static ObjectMapper objectMapper = new ObjectMapper();

    public final static String TOPIC_NAME = "rawtweets";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweethashtagscounterappkk");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> tweets = builder.stream(TOPIC_NAME);

        tweets.flatMapValues(value -> Collections.singletonList(getHashtags(value)))
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .print(Printed.toSysOut());

        Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

    }

    public static String getHashtags(String input) {
        JsonNode root;
        try {
            root = objectMapper.readTree(input);
            JsonNode hashtagsNode = root.path("entities").path("hashtags");
            if (!"[]".equals(hashtagsNode.toString())) {
                return hashtagsNode.get(0).path("text").asText();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return StringUtils.EMPTY;
    }

}
