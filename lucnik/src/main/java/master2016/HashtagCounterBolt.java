package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class HashtagCounterBolt extends BaseRichBolt {
    private HashMap<String, HashMap<String, Integer>> openLangCounterMap;
    private HashMap<String, HashMap<String, Integer>> closedLangCounterMap;

    private HashMap<String, Boolean> languageWindow;
    private HashMap<String, String> languageKeyword;
    private HashMap<String, Integer> langWindowNumber;

    private OutputCollector collector;

    public HashtagCounterBolt(String langList) {
        this.languageWindow = new HashMap<>();
        this.languageKeyword = new HashMap<>();
        this.openLangCounterMap = new HashMap<>();
        this.closedLangCounterMap = new HashMap<>();
        this.langWindowNumber = new HashMap<>();

        String[] langWithKeywords = langList.split(",");
        for (String langKeyword : langWithKeywords) {
            String[] split = langKeyword.split(":");
            String lang = split[0];
            String keyword = split[1];

            this.languageWindow.put(lang, false);
            this.languageKeyword.put(lang, keyword);
            this.langWindowNumber.put(lang, 0);
            this.openLangCounterMap.put(lang, new HashMap<String, Integer>());
        }
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String lang = tuple.getStringByField("lang");
        String keyword = this.languageKeyword.get(lang);
        String hashtag = tuple.getStringByField("hashtag");

        if (keyword.equals(hashtag)) {
            if (!this.languageWindow.get(lang)) {  // if the window was previously closed
                this.languageWindow.put(lang, true);
            } else {
                this.langWindowNumber.put(lang, this.langWindowNumber.get(lang) + 1);
                // close and save current window in the old one
                HashMap<String, Integer> closingCounterMap = this.openLangCounterMap.get(lang);

                HashMap<String, Integer> tmpCounterMap = new HashMap<>();
                for (Map.Entry<String, Integer> hashtagCount : closingCounterMap.entrySet()) {
                    tmpCounterMap.put(hashtagCount.getKey(), hashtagCount.getValue());
                }
                this.closedLangCounterMap.put(lang, tmpCounterMap);
                closingCounterMap.clear();
                System.out.println("CLEANED " + this.openLangCounterMap.get(lang).keySet());
                // cleanup();
                System.out.println("EMITIT");
                this.collector.emit(new Values(lang, tmpCounterMap, this.langWindowNumber.get(lang)));
            }
        } else {
            // update counter for that language
            HashMap<String, Integer> counterMap = this.openLangCounterMap.get(lang);
            if (!counterMap.containsKey(hashtag)) {
                counterMap.put(hashtag, 1);
            } else {
                Integer c = counterMap.get(hashtag) + 1;
                counterMap.put(hashtag, c);
            }
        }
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "map", "windowNumber"));
    }
}
