package master2016;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class OutputWriterBolt extends BaseRichBolt {
    private static final int N_RESULT = 3;
    private static final String GROUP_ID = "03";
    private final String outputFolder;
    private OutputCollector collector;
    private String langList;
    private HashMap<String, PrintWriter> langWriter;

    public OutputWriterBolt(String langList, String outputFolder) throws IOException {
        this.outputFolder = Paths.get(outputFolder).toString();
        this.langList = langList;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.langWriter = new HashMap<>();

        // TODO is it working with UTF-8
        String[] langs = this.langList.split(",");
        for (String langKeyword : langs) {
            String lang = langKeyword.split(":")[0];

            File outputPath = Paths.get(this.outputFolder, lang + "_" + OutputWriterBolt.GROUP_ID + ".log").toFile();

            FileWriter fw = null;
            try {
                fw = new FileWriter(outputPath);
            } catch (IOException e) {
                System.out.println("ERRLANG " + lang);
                e.printStackTrace();
            }
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw, true);
            this.langWriter.put(lang, out);
        }
    }

    public void execute(Tuple tuple) {
        System.out.println("SAVEIT");

        String lang = tuple.getStringByField("lang");
        HashMap<String, Integer> counterMap = (HashMap<String, Integer>) tuple.getValueByField("map");
        int windowNumber = (int) tuple.getValueByField("windowNumber");

        String[] hashtags = new String[OutputWriterBolt.N_RESULT];
        int[] counts = new int[OutputWriterBolt.N_RESULT];

        ArrayList<String> hashtagsIter = new ArrayList<>(counterMap.keySet());
        Collections.sort(hashtagsIter);
        System.out.println("SORTED\t" + hashtagsIter);

        // instead of ordering O(nlogn) simply look for the 3 most present hashtags each time
        // TODO in case of tie wins the alphabetical order => order the map..
        for (int i = 0; i < OutputWriterBolt.N_RESULT; i++) {
            String hashtag = "null";
            int count = 0;

            for (String hashtagIter : hashtagsIter) {
                int hashtagCount = counterMap.get(hashtagIter);
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

        // TODO what if tweets for a given language are not found? should we create the file anyway?
        // TODO shall we initialize langCounterMap with the list of languages?

        System.out.println(windowNumber + "," + lang + "," + r);
        this.langWriter.get(lang).println(windowNumber + "," + lang + "," + r);
        System.out.println("ACKED");
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
