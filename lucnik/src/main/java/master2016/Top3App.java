package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Top3App {
    public static void main(String[] args) throws Exception {
        String langlist;
        String kafkaBrokerUrls;
        String topologyName;
        String outputFolder;

        if (args.length == 0) {
            // TODO only for debug, then remove it
            // langlist = "en:2016MAMA";
            langlist = "en:ALDUBTwinsFever,it:natale,de:weihnachten,es:navidad";
            kafkaBrokerUrls = "localhost:9092";
            topologyName = "hello-storm";
            outputFolder = "/tmp/";
        } else {
            langlist = args[1];
            kafkaBrokerUrls = args[2];
            topologyName = args[3];
            outputFolder = args[4];
        }


        Config config = new Config();

        config.setDebug(true);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-twitter-spout", new KafkaTweetsSpout(kafkaBrokerUrls));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(langlist)).shuffleGrouping("kafka-twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt(langlist))
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("lang"));

        builder.setBolt("output-writer-bolt", new OutputWriterBolt(langlist, outputFolder))
                .fieldsGrouping("twitter-hashtag-counter-bolt", new Fields("lang"));


        // builder.setBolt("debug-bolt", new DebugBolt())
        // .shuffleGrouping("twitter-spout");

        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology(topologyName, config, builder.createTopology());
        // Thread.sleep(60 * 1000);
        // cluster.shutdown();

        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
    }
}
