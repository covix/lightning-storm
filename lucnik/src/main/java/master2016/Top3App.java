package master2016;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class Top3App {
    public static void main(String[] args) throws Exception {
        String langlist = args[0];
        String kafkaBrokerUrls = args[1];
        String topologyName = args[2];
        String outputFolder = args[3];

        Config config = new Config();
        config.put("topology.max.spout.pending", 1);

        TopologyBuilder builder = new TopologyBuilder();

        String[] langKeywords = langlist.split(",");
        for (String langKeyword : langKeywords) {
            String[] langKeywordSplitted = langKeyword.split(":");
            String lang = langKeywordSplitted[0];
            String keyword = langKeywordSplitted[1];

            builder.setSpout(lang + "-kafka-twitter-spout", new KafkaTweetsSpout(kafkaBrokerUrls, lang));

            builder.setBolt(lang + "-twitter-hashtag-reader-bolt", new HashtagReaderBolt(keyword, lang, outputFolder))
                    .shuffleGrouping(lang + "-kafka-twitter-spout");
        }

        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
    }
}
