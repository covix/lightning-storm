package master2016;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;


public class HashtagReaderBolt extends BaseRichBolt {
    private static final int N_RESULT = 3;
    private static final String GROUP_ID = "03";

    private String keyword;
    private OutputCollector collector;
    private Object2IntOpenHashMap<String> counterMap;
    private boolean windowOpen;
    private final String outputFolder;
    private final String language;
    private int windowCount;
    private PrintWriter writer;

    public HashtagReaderBolt(String keyword, String language, String outputFolder) {
        this.keyword = keyword;
        this.windowOpen = false;
        this.language = language;
        this.outputFolder = Paths.get(outputFolder).toString();
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counterMap = new Object2IntOpenHashMap<>();

        this.windowCount = 1;

        File outputPath = Paths.get(this.outputFolder, this.language + "_" + HashtagReaderBolt.GROUP_ID + ".log").toFile();
        FileWriter fw = null;
        try {
            fw = new FileWriter(outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw, true);

        this.writer = out;
    }

    public void execute(Tuple tuple) {
        String hashtag = tuple.getStringByField("hashtag");

        if (this.keyword.equals(hashtag)) {
            if (!this.windowOpen) {  // if the window was closed
                this.windowOpen = true;
            } else {
                appendToFile();
                this.counterMap = new Object2IntOpenHashMap<>();
            }
        } else {
            if (this.windowOpen) {
                if (!this.counterMap.containsKey(hashtag)) {
                    this.counterMap.put(hashtag, 1);
                } else {
                    this.counterMap.put(hashtag, this.counterMap.getInt(hashtag) + 1);
                }
            }
        }

        this.collector.ack(tuple);
    }

    private void appendToFile() {
        String[] hashtags = new String[HashtagReaderBolt.N_RESULT];
        int[] counts = new int[HashtagReaderBolt.N_RESULT];

        ArrayList<String> hashtagsIter = new ArrayList<>(this.counterMap.keySet());
        Collections.sort(hashtagsIter);

        // instead of ordering O(nlogn) simply look for the 3 most present hashtags each time
        for (int i = 0; i < HashtagReaderBolt.N_RESULT; i++) {
            String hashtag = "null";
            int count = 0;

            for (String hashtagIter : hashtagsIter) {
                int hashtagCount = this.counterMap.getInt(hashtagIter);
                if (hashtagCount > count) {
                    hashtag = hashtagIter;
                    count = hashtagCount;
                }
            }
            hashtags[i] = hashtag;
            counts[i] = count;
            hashtagsIter.remove(hashtag);
        }

        String r = "";
        for (int i = 0; i < hashtags.length; i++) {
            r += hashtags[i] + "," + counts[i] + ",";
        }
        r = r.substring(0, r.length() - 1);

        this.writer.println(this.windowCount + "," + this.language + "," + r);
        this.windowCount += 1;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
