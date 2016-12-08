package master2016;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HashtagCounterBolt extends BaseRichBolt {
    private HashMap<String, HashMap<String, Integer>> langCounterMap;
    private OutputCollector collector;
    private static final int N_RESULT = 3;
    private static final String GROUP_ID = "03";

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.langCounterMap = new HashMap<>();
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String lang = tuple.getStringByField("lang");

        if (!langCounterMap.containsKey(lang)) {
            langCounterMap.put(lang, new HashMap<String, Integer>());
        } else {
            HashMap<String, Integer> counterMap = langCounterMap.get(lang);
            String hashtag = tuple.getStringByField("hashtag");

            if (!counterMap.containsKey(hashtag)) {
                counterMap.put(hashtag, 1);
            } else {
                Integer c = counterMap.get(hashtag) + 1;
                counterMap.put(hashtag, c);
            }

        }
        collector.ack(tuple);
    }

    public void cleanup() {
        System.out.println("output");
        int windowNumber = -1;

        for (Map.Entry<String, HashMap<String, Integer>> entry : langCounterMap.entrySet()) {
            Map<String, Integer> counterMap = entry.getValue();
            String lang = entry.getKey();

            String[] hashtags = new String[N_RESULT];
            int[] counts = new int[N_RESULT];

            // instead of ordering O(nlogn) simply look for the 3 most present hashtags each time
            // TODO in case of tie wins the alphabetical order => order the map..
            for (int i = 0; i < N_RESULT; i++) {
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
            System.out.println(windowNumber + "," + lang + "," + r);
            try {
                PrintWriter writer = new PrintWriter(lang + "_" + GROUP_ID + ".log", "UTF-8");
                writer.println(windowNumber + "," + lang + "," + r);
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
