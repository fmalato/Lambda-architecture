import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

import java.util.Iterator;
import java.util.Map;

public class RetweetTextExtractorBolt implements IRichBolt {

    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {

        Status status = (Status)tuple.getValueByField("status");
        String title = status.getText();
        boolean isRetweet = status.isRetweet();
        String retweetScreenName = "";
        String retweetUserName = "";


        if(isRetweet) {
            Status retweet = status.getRetweetedStatus();
            retweetScreenName = retweet.getUser().getScreenName();
            retweetUserName = retweet.getUser().getName();
        }
        else {
            retweetScreenName = "Not Available";
            retweetUserName = "Not Available";
        }

        this.collector.emit(new Values(title, isRetweet, retweetScreenName, retweetUserName));

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("title", "isRetweet", "retweetScreenName", "retweetUserName"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
