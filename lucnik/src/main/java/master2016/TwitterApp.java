package master2016;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TwitterApp {
    private static class Mode {
        static int LOGFILE = 1;
        static int TWITTER = 2;
    }

    private static KafkaProducer<String, String> prod;
    private static Future<RecordMetadata> _send;

    public static void main(String[] args) throws TwitterException, IOException, ExecutionException, InterruptedException {
        int mode = Integer.parseInt(args[0]);
        String consumerKey = args[1];
        String consumerSecret = args[2];
        String accessToken = args[3];
        String accessTokenSecret = args[4];
        String kafkaBrokerUrls = args[5];
        String filename = args[6];

        initKafkaProducer(kafkaBrokerUrls);

        if (mode == Mode.TWITTER) {
            readFromTwitter(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        } else if (mode == Mode.LOGFILE) {
            readFromLogFile(filename);
        } else {
            throw new IllegalArgumentException("Mode not supported");
        }
    }

    private static void initKafkaProducer(String brokerUrls) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrls);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        prod = new KafkaProducer<>(props);
    }

    private static void readFromLogFile(String filename) throws IOException, ExecutionException, InterruptedException, TwitterException {
        BufferedReader tfbr = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = tfbr.readLine()) != null) {
            // writeTweetToKafka(line, true);
            Status status = TwitterObjectFactory.createStatus(line);
            writeHashtagsToKafka(status, true);
        }
        _send.get();
        closeKafkaProductors();
    }

    private static void readFromTwitter(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        TwitterStream twitterStream;
        twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                try {
                    writeHashtagsToKafka(status);
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onScrubGeo(long userId, long upToStatusId) {
            }

            public void onStallWarning(StallWarning warning) {
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);

        twitterStream.sample();
    }

    private static void writeHashtagsToKafka(Status tweet) throws ExecutionException, InterruptedException {
        writeHashtagsToKafka(tweet, false);
    }

    private static void writeHashtagsToKafka(Status tweet, boolean wait) throws ExecutionException, InterruptedException {
        String topic = tweet.getLang();
        int partition = 0;
        String key = null;

        for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
            Future<RecordMetadata> send = prod.send(new ProducerRecord<>(topic, partition, key, hashtag.getText()));

            if (wait) {
                _send = send;
            }
        }
    }


    private static void closeKafkaProductors() {
        prod.close();
    }
}
