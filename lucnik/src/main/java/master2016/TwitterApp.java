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
import java.nio.file.Paths;
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
        int mode;
        String consumerKey;
        String consumerSecret;
        String accessToken;
        String accessTokenSecret;
        String kafkaBrokerUrls;
        String filename;

        if (args.length == 0) {
            // TODO remove after startTwitterApp.sh is finished
            // mode = Mode.TWITTER;
            mode = Mode.LOGFILE;

            consumerKey = "HZldFa2RQ8ByVPa5wTl7UKvQR";
            consumerSecret = "ZwhQSj37kpq6vCRRShBwfK32iB58QnrcidnJJvxF5vzzxPSISM";
            accessToken = "3936335896-DiNCA5l1tQabWe12V45yrARVG87bMGiHA9LzWBA";
            accessTokenSecret = "fjeAmE1ZiY6Z34v5ioc32yh49HklHYNyIGFanPxWvqImw";
            kafkaBrokerUrls = "localhost:9092";
            filename = Paths.get("data", "tweets.txt").toString();
        } else {
            mode = Integer.parseInt(args[0]);
            consumerKey = args[1];
            consumerSecret = args[2];
            accessToken = args[3];
            accessTokenSecret = args[4];
            kafkaBrokerUrls = args[5];
            filename = args[6];
        }
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
        Thread.sleep(10 * 1000);
        closeKafkaProductors();
    }

    private static void readFromTwitter(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        TwitterStream twitterStream;
        twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                // System.out.println(status.getLang() + ": " + "@" + status.getUser().getScreenName() + " - " + status.getText());
                String jsonStatus = TwitterObjectFactory.getRawJSON(status);
                try {
                    // writeTweetToKafka(jsonStatus);
                    writeHashtagsToKafka(status);
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                // System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                // System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
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
            // TODO remove check of languages
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
