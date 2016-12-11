package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;


public class HashtagCounterBolt extends BaseRichBolt {
    private static final int N_RESULT = 3;
    private static final String GROUP_ID = "03";
    private final String outputFolder;

    private HashMap<String, HashMap<String, Integer>> openLangCounterMap;
    private HashMap<String, HashMap<String, Integer>> closedLangCounterMap;

    private HashMap<String, Boolean> languageWindow;
    private HashMap<String, String> languageKeyword;

    private OutputCollector collector;
    private int windowNumber;

    public HashtagCounterBolt(String langList, String outputFolder) {
        // TODO Move definition to prepare
        this.languageWindow = new HashMap<>();
        this.languageKeyword = new HashMap<>();
        this.openLangCounterMap = new HashMap<>();
        this.closedLangCounterMap = new HashMap<>();
        this.windowNumber = 0;
        this.outputFolder = Paths.get(outputFolder).toString();

        String[] langWithKeywords = langList.split(",");
        for (String langKeyword : langWithKeywords) {
            String[] split = langKeyword.split(":");
            String lang = split[0];
            String keyword = split[1];

            this.languageWindow.put(lang, false);
            this.languageKeyword.put(lang, keyword);
            this.openLangCounterMap.put(lang, new HashMap<String, Integer>());
        }
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        // keyWord = (String) conf.get("my.keyWord");
    }

    public void execute(Tuple tuple) {
        String lang = tuple.getStringByField("lang");

        // TODO This is for debug, but it could be left here for robustness
        if (this.languageKeyword.containsKey(lang)) {
            String keyword = this.languageKeyword.get(lang);

            String hashtag = tuple.getStringByField("hashtag");

            if (keyword.equals(hashtag)) {
                this.windowNumber += 1;
                if (!languageWindow.get(lang)) {
                    this.languageWindow.put(lang, true);
                }

                if (this.languageWindow.get(lang)) {
                    // close and save current window in the old one
                    HashMap<String, Integer> closingCounterMap = this.openLangCounterMap.get(lang);

                    this.closedLangCounterMap.put(lang, new HashMap<String, Integer>());
                    HashMap<String, Integer> tmpCounterMap = this.closedLangCounterMap.get(lang);
                    for (Map.Entry<String, Integer> hashtagCount : closingCounterMap.entrySet()) {
                        tmpCounterMap.put(hashtagCount.getKey(), hashtagCount.getValue());
                    }
                    closingCounterMap.clear();
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
        }
        this.collector.ack(tuple);
    }

    public void cleanup() {
        for (Map.Entry<String, HashMap<String, Integer>> entry : this.openLangCounterMap.entrySet()) {
            Map<String, Integer> counterMap = entry.getValue();
            String lang = entry.getKey();

            String[] hashtags = new String[HashtagCounterBolt.N_RESULT];
            int[] counts = new int[HashtagCounterBolt.N_RESULT];

            // instead of ordering O(nlogn) simply look for the 3 most present hashtags each time
            // TODO in case of tie wins the alphabetical order => order the map..
            for (int i = 0; i < HashtagCounterBolt.N_RESULT; i++) {
                String hashtag = "null";
                int count = 0;

                for (Map.Entry<String, Integer> hashtagCount : counterMap.entrySet()) {
                    if (hashtagCount.getValue() > count) {
                        hashtag = hashtagCount.getKey();
                        count = hashtagCount.getValue();
                    }
                }
                hashtags[i] = hashtag;
                counts[i] = count;
                counterMap.remove(hashtag);
            }

            String r = "";
            for (int i = 0; i < hashtags.length; i++) {
                r += hashtags[i] + "," + counts[i] + ",";
            }
            r = r.substring(0, r.length() - 1);

            // TODO what if tweets for a given language are not found? should we create the file anyway?
            // TODO shall we initialize langCounterMap with the list of languages?

            System.out.println(this.windowNumber + "," + lang + "," + r);
            try {
                File outputPath = Paths.get(this.outputFolder, lang + "_" + HashtagCounterBolt.GROUP_ID + ".log").toFile();
                outputPath.getParentFile().mkdirs();
                PrintWriter writer = new PrintWriter(outputPath, "UTF-8");
                writer.println(this.windowNumber + "," + lang + "," + r);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }
}
