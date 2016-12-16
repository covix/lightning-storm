package master2016;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
        // TODO Move definition to prepare
        this.languageWindow = new HashMap<>();
        this.languageKeyword = new HashMap<>();

        String[] langWithKeywords = langlist.split(",");
        for (String langKeyword : langWithKeywords) {
            String[] split = langKeyword.split(":");
            String lang = split[0];
            String keyword = split[1];

            this.languageWindow.put(lang, false);
            this.languageKeyword.put(lang, keyword);
        }
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String lang = tuple.getStringByField("lang");
        String hashtag = tuple.getStringByField("hashtag");

        // TODO This is for debug, but it could be left here for robustness
        if (this.languageKeyword.containsKey(lang)) {
            final String keyword = this.languageKeyword.get(lang);

            if (new Random().nextDouble() > .9) {
                System.out.println("Fake it 'till you make it");
                hashtag = keyword;
            }

            System.out.println("#### TEST " + keyword + " " + hashtag);

            if (lang.equals("en")) {
                System.out.println("LANGIT\t" + hashtag);
            }

            if (keyword.equals(hashtag)) {
                // there's no need to stop the window (a closing keyword is also an opening
                // languageWindow.put(lang, !languageWindow.get(lang));
                this.languageWindow.put(lang, true);
                System.out.println("WINDOW: " + this.languageWindow.get(lang));
            }

            if (this.languageWindow.get(lang)) {
                this.collector.emit(new Values(lang, hashtag));
                System.out.println("I EMIT: " + hashtag);
            }
        }
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "hashtag"));
    }
}
