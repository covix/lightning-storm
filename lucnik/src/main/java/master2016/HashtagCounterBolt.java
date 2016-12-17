package master2016;

import it.unimi.dsi.fastutil.objects.Object2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;


public class HashtagCounterBolt extends BaseRichBolt {
    private HashMap<String, Object2IntOpenHashMap<String>> openLangCounterMap;

    private Object2BooleanOpenHashMap<String> languageWindow;
    private Object2IntOpenHashMap<String> languageKeywordIndex;
    private String[] languageKeyword;

    private OutputCollector collector;

    public HashtagCounterBolt(String langList) {
        this.languageWindow = new Object2BooleanOpenHashMap<>();
        this.languageKeywordIndex = new Object2IntOpenHashMap<>();
        this.openLangCounterMap = new HashMap<>();

        String[] langWithKeywords = langList.split(",");
        this.languageKeyword = new String[langWithKeywords.length];

        for (int i = 0; i < langWithKeywords.length; i++) {
            String langKeyword = langWithKeywords[i];
            String[] split = langKeyword.split(":");
            String lang = split[0];
            String keyword = split[1];

            this.languageWindow.put(lang, false);
            this.languageKeywordIndex.put(lang, i);
            this.languageKeyword[i] = keyword;
            this.openLangCounterMap.put(lang, new Object2IntOpenHashMap<String>());
        }
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String lang = tuple.getStringByField("lang");
        String keyword = this.languageKeyword[this.languageKeywordIndex.getInt(lang)];
        String hashtag = tuple.getStringByField("hashtag");

        System.out.println("COUNTERR: "+ hashtag);

        if (keyword.equals(hashtag)) {
            if (!this.languageWindow.getBoolean(lang)) {  // if the window was previously closed
                this.languageWindow.put(lang, true);
            } else {
                Object2IntOpenHashMap<String> closingCounterMap = this.openLangCounterMap.get(lang);

                Object2IntOpenHashMap<String> tmpCounterMap = new Object2IntOpenHashMap<>();
                for (Map.Entry<String, Integer> hashtagCount : closingCounterMap.entrySet()) {
                    tmpCounterMap.put(hashtagCount.getKey(), (int) hashtagCount.getValue());
                }
                closingCounterMap.clear();
                System.out.println("MAPP: " + tmpCounterMap);
                this.collector.emit(new Values(lang, tmpCounterMap));
            }
            System.out.println("COUNTING: " + hashtag + " : " + "KEYWORD");
        } else {
            // update counter for that language
            Object2IntOpenHashMap<String> counterMap = this.openLangCounterMap.get(lang);
            if (!counterMap.containsKey(hashtag)) {
                counterMap.put(hashtag, 1);
            } else {
                int c = counterMap.getInt(hashtag) + 1;
                counterMap.put(hashtag, c);
            }
            System.out.println("COUNTING: " + hashtag + " : " + counterMap.getInt(hashtag));
        }
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "map"));
    }
}
