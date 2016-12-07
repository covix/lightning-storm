package readDoc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitterBolt implements IRichBolt {
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String sentence = input.getString(0);
		getHT(sentence);
		getLang(sentence);
		String sentence2 = getText(sentence);
		String[] words = sentence2.split(" ");
		for (String word : words) {

			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
		collector.ack(input);
	}

	private String getText(String sentence) {
		String text = "";
		String wordToFind1 = "text" + "\"" + ":";
		String wordToFind2 = "\"" + ",";

		int start = 0;
		int stop = 0;
		Pattern worda = Pattern.compile(wordToFind1);
		Matcher match = worda.matcher(sentence);
		while (match.find() && start == 0) {
			start = match.end() + 1;
		}
		String textNew = sentence.substring(start);

		Pattern wordB = Pattern.compile(wordToFind2);
		Matcher matchB = wordB.matcher(textNew);

		while (matchB.find() && stop == 0) {
			stop = matchB.end();
		}

		text = textNew.substring(0, stop - 2);
		System.out.println("Textbody: " + text);
		return text;
	}

	private String getLang(String sentence) {
		String text = "";
		String wordToFind = "lang";
		int stop = 0;
		Pattern worda = Pattern.compile(wordToFind);
		Matcher match = worda.matcher(sentence);
		while (match.find()) {
			stop = match.end();
		}
		text = sentence.substring(stop + 3, stop + 5);
		System.out.println("Language: " + text);
		return text;
	}

	private String getHT(String sentence) {
		System.out.println("##################");
		List<String> hashtags = new ArrayList<String>();
		String text = "";
		String wordToFind1 = "hashtags" + "\"" + ":";
		String wordToFind2 = "\"" + ",";
		String wordToFind3 = "text" + "\"" + ":";
		int start = 0;
		int stop = 0;
		int limit=0;		
		Pattern worda = Pattern.compile(wordToFind1);
		Matcher match = worda.matcher(sentence);
		while (match.find() && start == 0) {
			start = match.end() + 3;
		}
				
		String alt = sentence.substring(start);
		
		Pattern wordD = Pattern.compile(wordToFind1);
		Matcher matchD = wordD.matcher(alt);
		while (matchD.find()) {
			limit = matchD.end();
		}
		String textNew = alt.substring(0,limit);

		Pattern wordC = Pattern.compile(wordToFind3);
		Matcher matchC = wordC.matcher(textNew);
		while (matchC.find()) {
			start = start + 7;
			Pattern wordB = Pattern.compile(wordToFind2);
			Matcher matchB = wordB.matcher(textNew);
			while (matchB.find() && stop == 0) {
				stop = matchB.end() - 2;
			}
			text = textNew.substring(7, stop);
			hashtags.add(text);
		}

		System.out.println("#s: " + hashtags);
		return null;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public void cleanup() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}