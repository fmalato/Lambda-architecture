import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class OfflineTweetTextExtractorBolt implements IRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {

        String id = tuple.getStringByField("tweet-id");
        String date = tuple.getStringByField("date");
        String text = tuple.getStringByField("text");
        String[] words = text.split(" ");

        this.collector.emit(new Values(id, date, words));

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("tweet-id", "date", "words"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
