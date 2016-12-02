package master2016;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterApp {
    private static class Mode {
        static int LOGFILE = 1;
        static int TWITTER = 2;
    }

    public static void main(String[] args) throws TwitterException, FileNotFoundException, UnsupportedEncodingException {
        TwitterStream twitterStream;

        int mode;
        String consumerKey;
        String consumerSecret;
        String accessToken;
        String accessTokenSecret;
        String kafkaBrokerUrl;
        String filename;


        if (args.length == 0) {
            // TODO remove after startTwitterApp.sh is finished
            mode = 1;
            consumerKey = "HZldFa2RQ8ByVPa5wTl7UKvQR";
            consumerSecret = "ZwhQSj37kpq6vCRRShBwfK32iB58QnrcidnJJvxF5vzzxPSISM";
            accessToken = "3936335896-DiNCA5l1tQabWe12V45yrARVG87bMGiHA9LzWBA";
            accessTokenSecret = "fjeAmE1ZiY6Z34v5ioc32yh49HklHYNyIGFanPxWvqImw";
            kafkaBrokerUrl = null;
            filename = "./data/tweets.txt";
        } else {
            mode = Integer.parseInt(args[1]);
            consumerKey = args[2];
            consumerSecret = args[3];
            accessToken = args[4];
            accessTokenSecret = args[5];
            kafkaBrokerUrl = args[6];
            filename = args[7];
        }

        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                // TODO save tweets to kafka
                System.out.println(status.getLang() + ": " + "@" + status.getUser().getScreenName() + " - " + status.getText());
                String rawJSON = TwitterObjectFactory.getRawJSON(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        // TODO create some kind of reader based on the mode
        if (mode == Mode.TWITTER) {
            twitterStream = new TwitterStreamFactory(
                    new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                    .getInstance();

            twitterStream.addListener(listener);
            twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
            AccessToken token = new AccessToken(accessToken, accessTokenSecret);
            twitterStream.setOAuthAccessToken(token);

            FilterQuery tweetFilterQuery = new FilterQuery();
            // TODO read actual tweets (not sample)
            twitterStream.sample();
        } else if (mode == Mode.LOGFILE) {
            // TODO read from logfile
            throw new NotImplementedException();
        } else {
            throw new IllegalArgumentException("Mode not supported");
        }
    }
}
