import com.esotericsoftware.kryo.util.Util;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class OfflineTweetClassifierBolt implements IRichBolt {

    private OutputCollector collector;
    private ArrayList<VocabularyEntry> vocabularyEng;
    private ArrayList<VocabularyEntry> vocabularyIt;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.vocabularyEng = new ArrayList<VocabularyEntry>();
        this.vocabularyIt = new ArrayList<VocabularyEntry>();
        JSONParser parser= new JSONParser();


        try {
            Object obj = parser.parse(new FileReader("vocabularyEng.json"));
            Object objIt = parser.parse(new FileReader("vocabularyIt.json"));
            JSONObject jsonObj = (JSONObject) obj;
            JSONObject jsonObjIt = (JSONObject) objIt;
            Iterator iteratorEng = jsonObj.keySet().iterator();
            Iterator iteratorIt = jsonObjIt.keySet().iterator();

            while(iteratorEng.hasNext()) {
                String currentKey = iteratorEng.next().toString();
                ArrayList<Double> values = (ArrayList<Double>) jsonObj.get(currentKey);
                Double doubleScore = Utilities.getScore(values);
                int score = doubleScore.intValue();
                this.vocabularyEng.add(new VocabularyEntry(currentKey, score));
            }
            while(iteratorIt.hasNext()) {
                String currentKey = iteratorIt.next().toString();
                ArrayList<Double> values = (ArrayList<Double>) jsonObj.get(currentKey);
                Double doubleScore = Utilities.getScore(values);
                int score = doubleScore.intValue();
                this.vocabularyIt.add(new VocabularyEntry(currentKey, score));
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
            if(Utilities.containedIn(word, this.vocabularyEng)) {
                score += Utilities.returnScore(word, this.vocabularyEng);
            }
            else if(Utilities.containedIn(word, this.vocabularyIt)) {
                score += Utilities.returnScore(word, this.vocabularyIt);
            }
        }

        this.collector.emit(new Values(id, date, Arrays.toString(words), score));

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
