package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Top3App {
    public static void main(String[] args) throws Exception {
        String langlist;
        String kafkaBrokerUrl;
        String topologyName;
        String outputFolder;

        if (args.length == 0) {
            // TODO only for debug, then remove it

            langlist = "en:2016MAMA";
            // langlist = "en:christmas,it:natale,de:weihnachten,es:navidad";

            // TODO need to start using it
            kafkaBrokerUrl = "";
            topologyName = "hello-storm";

            // TODO pass to the hashtag counter bolt
            outputFolder = "output";
        } else {
            langlist = args[1];
            kafkaBrokerUrl = args[2];
            topologyName = args[3];
            outputFolder = args[4];
        }

        // String[] langWithKeywords = langlist.split(",");

        Config config = new Config();

        // Window try
        // config.put("my.keyWord", keyWord);

        config.setDebug(false);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-twitter-spout", new KafkaTweetsSpout());

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(langlist)).shuffleGrouping("kafka-twitter-spout");

        builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt(langlist))
                .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("lang"));

        builder.setBolt("output-writer-bolt", new OutputWriterBolt(langlist, outputFolder))
                .fieldsGrouping("twitter-hashtag-counter-bolt", new Fields("lang"));


        // builder.setBolt("debug-bolt", new DebugBolt())
        // .shuffleGrouping("twitter-spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, config, builder.createTopology());
        Thread.sleep(30 * 1000);
        cluster.shutdown();

        // StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
    }
}
