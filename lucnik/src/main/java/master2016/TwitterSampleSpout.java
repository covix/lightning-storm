package master2016;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class TwitterSampleSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private LinkedBlockingQueue<Status> queue = null;
    private TwitterStream _twitterStream;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String[] keyWords;

    public TwitterSampleSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, String[] keyWords) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keyWords = keyWords;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;

        StatusListener listener = new StatusListener() {

            public void onStatus(Status status) {

                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            public void onTrackLimitationNotice(int i) {
            }

            public void onScrubGeo(long l, long l1) {
            }

            public void onException(Exception ex) {
            }

            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub

            }

        };

        _twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        _twitterStream.addListener(listener);
        _twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        _twitterStream.setOAuthAccessToken(token);

        if (keyWords.length == 0) {

            _twitterStream.sample();
        } else {

            FilterQuery query = new FilterQuery().track(keyWords);
            _twitterStream.filter(query);
        }

    }

    public void nextTuple() {
        Status ret = queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));

        }
    }

    public void close() {
        _twitterStream.shutdown();
    }

    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}