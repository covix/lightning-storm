package master2016;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
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
            langlist = "en:ALDUBTwinsFever,jp:morgan,de:weihnachten,es:navidad";
            kafkaBrokerUrls = "localhost:9092";
            topologyName = "hello-storm";
            outputFolder = "/tmp/";
        } else {
            langlist = args[0];
            kafkaBrokerUrls = args[1];
            topologyName = args[2];
            outputFolder = args[3];
        }

        Config config = new Config();
        // config.put("topology.max.spout.pending", Integer.valueOf(1));
        TopologyBuilder builder = new TopologyBuilder();

        String[] languages = langlist.split(",");
        for (String language : languages) {
            String[] langKeyword = language.split(":");
            String lang = langKeyword[0];
            String keyword = langKeyword[1];

            builder.setSpout(lang + "-kafka-twitter-spout", new KafkaTweetsSpout(kafkaBrokerUrls, lang));

            builder.setBolt(lang + "-twitter-hashtag-reader-bolt", new HashtagReaderBolt(keyword))
                    .shuffleGrouping(lang + "-kafka-twitter-spout");

            builder.setBolt(lang + "-twitter-hashtag-counter-bolt", new HashtagCounterBolt(keyword))
                    .shuffleGrouping(lang + "-twitter-hashtag-reader-bolt");

            builder.setBolt(lang + "-output-writer-bolt", new OutputWriterBolt(lang, outputFolder))
                    .shuffleGrouping(lang + "-twitter-hashtag-counter-bolt");
        }

        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology(topologyName, config, builder.createTopology());
        // Thread.sleep(60 * 1000);
        // cluster.shutdown();

        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
    }
}
