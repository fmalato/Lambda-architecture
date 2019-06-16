import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class OfflineTweetClassifierBolt implements IRichBolt {

    private OutputCollector collector;
    private ArrayList<VocabularyEntry> vocabulary;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.vocabulary = new ArrayList<VocabularyEntry>();
        String line;
        String[] lineArray;
        JSONParser parser= new JSONParser();

        try {
            Object obj = parser.parse(new FileReader("vocabulary.json"));
            JSONObject jsonObj = (JSONObject) obj;

            for(Iterator iterator = jsonObj.keySet().iterator(); iterator.hasNext(); iterator.next()) {
                ArrayList<Float> values = (ArrayList<Float>)jsonObj.get(iterator.toString());
                int score = Utilities.getScore(values);
                this.vocabulary.add(new VocabularyEntry(iterator.toString(), score));
            }
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        } catch(ParseException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {

        String id = tuple.getStringByField("tweet-id");
        String date = tuple.getStringByField("date");
        String[] words = (String[]) tuple.getValueByField("words");
        int score = 0;

        for (String word : words) {
            if(Utilities.containedIn(word, this.vocabulary)) {
                score = Utilities.returnScore(word, this.vocabulary);
            }
        }

        this.collector.emit(new Values(id, date, words.toString(), score));

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet-id", "date", "text", "score"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
