package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Top3App {
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();
        // builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
        //         consumerSecret, accessToken, accessTokenSecret, keyWords));
        //
        // builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
        //         .shuffleGrouping("twitter-spout");
        //
        // builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
        //         .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));

        builder.setSpout("twitter-spout", new KafkaTweetsSpout());
        builder.setBolt("debug-bolt", new DebugBolt())
                .shuffleGrouping("twitter-spout");


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TwitterHashtagStorm", config,
                builder.createTopology());
        Thread.sleep(20000);
        cluster.shutdown();
    }
}
