package master2016;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

public class TwitterApp {
    private static class Mode {
        static int LOGFILE = 1;
        static int TWITTER = 2;
    }

    private static KafkaProducer<String, String> prod;

    public static void main(String[] args) throws TwitterException, IOException {
        int mode;
        String consumerKey;
        String consumerSecret;
        String accessToken;
        String accessTokenSecret;
        String kafkaBrokerUrl;
        String filename;

        if (args.length == 0) {
            // TODO remove after startTwitterApp.sh is finished
            // mode = Mode.TWITTER;
            mode = Mode.LOGFILE;

            consumerKey = "HZldFa2RQ8ByVPa5wTl7UKvQR";
            consumerSecret = "ZwhQSj37kpq6vCRRShBwfK32iB58QnrcidnJJvxF5vzzxPSISM";
            accessToken = "3936335896-DiNCA5l1tQabWe12V45yrARVG87bMGiHA9LzWBA";
            accessTokenSecret = "fjeAmE1ZiY6Z34v5ioc32yh49HklHYNyIGFanPxWvqImw";
            kafkaBrokerUrl = null;
            filename = Paths.get("data", "tweets.txt").toString();
        } else {
            mode = Integer.parseInt(args[1]);
            consumerKey = args[2];
            consumerSecret = args[3];
            accessToken = args[4];
            accessTokenSecret = args[5];
            kafkaBrokerUrl = args[6];
            filename = args[7];
        }

        initKafkaProducer();

        if (mode == Mode.TWITTER) {
            readFromTwitter(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        } else if (mode == Mode.LOGFILE) {
            readFromLogFile(filename);
        } else {
            throw new IllegalArgumentException("Mode not supported");
        }
    }

    private static void initKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        prod = new KafkaProducer<>(props);
    }

    private static void readFromLogFile(String filename) throws IOException {
        BufferedReader tfbr = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = tfbr.readLine()) != null) {
            writeTweetToKafka(line);
        }
        closeKafkaProductor();
    }

    private static void readFromTwitter(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        // TODO should it ever end? If so, call close!
        TwitterStream twitterStream;
        twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                // System.out.println(status.getLang() + ": " + "@" + status.getUser().getScreenName() + " - " + status.getText());
                String jsonStatus = TwitterObjectFactory.getRawJSON(status);
                writeTweetToKafka(jsonStatus);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                // System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                // System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) {
                // System.out.println("Got stall warning:" + warning);
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        twitterStream.addListener(listener);
        twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        twitterStream.setOAuthAccessToken(token);

        // FilterQuery tweetFilterQuery = new FilterQuery();
        // TODO read actual tweets (not sample). (it could also be right/enough).
        twitterStream.sample();
    }

    private static void writeTweetToKafka(String tweet) {
        String topic = "twitter";
        int partition = 0;
        // From the Docs:
        // The key is an optional message key that was used for partition assignment. The key can be null.
        String key = "testKey";

        // TODO check whether all tweets are written to kafka
        // adding .get() at the returned object will make the method synchronous.
        // without it, the application won't wait for it before terminating
        prod.send(new ProducerRecord<>(topic, partition, key, tweet));
    }

    private static void closeKafkaProductor() {
        prod.close();
    }
}
