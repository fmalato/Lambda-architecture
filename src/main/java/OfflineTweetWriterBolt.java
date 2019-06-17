import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class OfflineTweetWriterBolt implements IRichBolt {

    private OutputCollector collector;
    private BufferedWriter writer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        try {
            this.writer = new BufferedWriter(new FileWriter("db.txt"));
        } catch(IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {

        String id = tuple.getStringByField("tweet-id");
        String date = tuple.getStringByField("date");
        String text = tuple.getStringByField("text");
        Integer score = (Integer)tuple.getValueByField("score");
        String record = id + ", " + date + ", " + text + ", " + score.toString() + "\n";

        try {
            this.writer.write(record);
            this.writer.flush();

        } catch(IOException e) {
            e.printStackTrace();
        }

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
