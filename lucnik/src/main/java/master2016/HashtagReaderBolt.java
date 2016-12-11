package master2016;

import java.util.HashMap;
import java.util.Map;

import twitter4j.Status;
import twitter4j.HashtagEntity;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HashtagReaderBolt extends BaseRichBolt {
    private OutputCollector collector;

    private HashMap<String, Boolean> languageWindow;
    private HashMap<String, String> languageKeyword;

    public HashtagReaderBolt(String langlist) {
        languageWindow = new HashMap<>();
        languageKeyword = new HashMap<>();

        String[] langWithKeywords = langlist.split(",");
        for (String langKeyword : langWithKeywords) {
            String[] split = langKeyword.split(":");
            String lang = split[0];
            String keyword = split[1];

            languageWindow.put(lang, false);
            languageKeyword.put(lang, keyword);
        }
    }


    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        String lang = tweet.getLang();

        // TODO This is for debug, but it could be left here for robustness
        if (languageKeyword.containsKey(lang)) {
            String keyword = languageKeyword.get(lang);

            for (HashtagEntity hashtag : tweet.getHashtagEntities()) {
                // TODO shall we emit lowercase hashtags? YES! (at least for the internal comparison?
                System.out.println("Hashtag: " + hashtag.getText());
                System.out.println("#### TEST " + keyword + " " + hashtag.getText());

                if (keyword.equals(hashtag.getText().toLowerCase())) {
                    languageWindow.put(lang, !languageWindow.get(lang));
                    System.out.println("WINDOW: " + languageWindow.get(lang));
                }

                if (languageWindow.get(lang)) {
                    this.collector.emit(new Values(lang, hashtag.getText().toLowerCase()));
                    System.out.println("I EMIT: " + hashtag.getText());
                }

            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "hashtag"));
    }
}
