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

class OutputWriterBolt extends BaseRichBolt {
    private static final int N_RESULT = 3;
    private static final String GROUP_ID = "03";
    private final String outputFolder;
    private final String language;
    private OutputCollector collector;
    private int windowCount;
    private PrintWriter writer;

    public OutputWriterBolt(String language, String outputFolder) throws IOException {
        this.language = language;
        this.outputFolder = Paths.get(outputFolder).toString();
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.windowCount = 1;

        File outputPath = Paths.get(this.outputFolder, this.language + "_" + OutputWriterBolt.GROUP_ID + ".log").toFile();
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
        Object2IntOpenHashMap<String> counterMap = (Object2IntOpenHashMap<String>) tuple.getValueByField("map");

        System.out.println("RECMAP\t" + counterMap);

        String[] hashtags = new String[OutputWriterBolt.N_RESULT];
        int[] counts = new int[OutputWriterBolt.N_RESULT];

        System.out.print("[DEBUGG]\t");
        System.out.print("Keys: " + counterMap.keySet() + "\t");
        // System.out.print("counterMap equal null?: " + counterMap == null + "\t");
        // System.out.print("counterMap.keySet() == null?: " + counterMap.keySet() == null + "\t");

        ArrayList<String> hashtagsIter = new ArrayList<>(counterMap.keySet());
        System.out.print("Array: " + hashtagsIter + "\t");
        Collections.sort(hashtagsIter);
        System.out.println("Sorted Keys " + hashtagsIter+ "\t");

        // instead of ordering O(nlogn) simply look for the 3 most present hashtags each time
        for (int i = 0; i < OutputWriterBolt.N_RESULT; i++) {
            String hashtag = "null";
            int count = 0;

            for (String hashtagIter : hashtagsIter) {
                int hashtagCount = counterMap.getInt(hashtagIter);
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
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
