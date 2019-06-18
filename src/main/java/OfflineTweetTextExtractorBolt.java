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

        String text = tuple.getStringByField("text");
        String[] words = text.split(" ");

        this.collector.emit(new Values(words));

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("words"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
