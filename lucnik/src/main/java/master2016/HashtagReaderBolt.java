package master2016;

import it.unimi.dsi.fastutil.objects.Object2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class HashtagReaderBolt extends BaseRichBolt {
    private OutputCollector collector;

    // private HashMap<String, Boolean> languageWindow;
    // private HashMap<String, String> languageKeyword;
    private Object2BooleanOpenHashMap<String> languageWindow;
    private Object2IntLinkedOpenHashMap<String> languageKeywordIndex;
    private String[] languageKeyword;

    public HashtagReaderBolt(String langlist) {
        // TODO Move definition to prepare
        this.languageWindow = new Object2BooleanOpenHashMap<>();
        this.languageKeywordIndex = new Object2IntLinkedOpenHashMap<>();

        String[] langWithKeywords = langlist.split(",");
        this.languageKeyword = new String[langWithKeywords.length];

        for (int i = 0; i < langWithKeywords.length; i++) {
            String langKeyword = langWithKeywords[i];
            String[] split = langKeyword.split(":");
            String lang = split[0];
            String keyword = split[1];

            this.languageWindow.put(lang, false);
            this.languageKeywordIndex.put(lang, i);
            this.languageKeyword[i] = keyword;
        }
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String lang = tuple.getStringByField("lang");
        String hashtag = tuple.getStringByField("hashtag");

        System.out.println("READERR: "+ hashtag);

        String keyword = this.languageKeyword[this.languageKeywordIndex.getInt(lang)];

        if (!this.languageWindow.getBoolean(lang)) {
            if (keyword.equals(hashtag)) {
                this.languageWindow.put(lang, true);
                this.collector.emit(new Values(lang, hashtag));
            }
        } else {
            this.collector.emit(new Values(lang, hashtag));
        }
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "hashtag"));
    }
}
