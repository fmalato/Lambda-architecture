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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class TweetClassifierBolt implements IRichBolt {

    private OutputCollector collector;
    private ArrayList<VocabularyEntry> vocabularyEng;
    private ArrayList<VocabularyEntry> vocabularyIt;

    /*** The prepare() method provide an initialization of the vocabularies, each as an ArrayList of VocabularyEntries.
     * We have chosen this method to avoid multiple calls to the JSONObject.get() method and to have a clearer structure
     * of the objects.
     */

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
                ArrayList<Double> values = (ArrayList<Double>) jsonObjIt.get(currentKey);
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

    /*** The TweetClassifierBolt splits the retrieved tweet text in single words, getting also rid of the punctuation
     * and of the special characters. Each word is then compared with the vocabulary entries. If there's a match, the
     * Utilities.returnScore() call updates the word's score. Otherwise, the score is set to zero.
     */

    @Override
    public void execute(Tuple tuple) {

        String text = (String)tuple.getValueByField("text");
        String[] words = text.split("\\s+");
        for (int i = 0; i < words.length; i++) {
            words[i] = words[i].replaceAll("[^\\w]", "");
        }
        int score = 0;

        for (String word : words) {
            if(Utilities.containedIn(word, this.vocabularyEng)) {
                score += Utilities.returnScore(word, this.vocabularyEng);
            }
            else if(Utilities.containedIn(word, this.vocabularyIt)) {
                score += Utilities.returnScore(word, this.vocabularyIt);
            }
        }

        this.collector.emit(new Values(score));

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("score"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
