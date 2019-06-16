import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class OfflineTwitterStreamSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private ArrayList<String> lineBuffer;
    private Iterator lineBufferItr;
    private Utilities utils = new Utilities();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.collector = spoutOutputCollector;
        String dataPath = "data.csv";
        this.lineBuffer = new ArrayList<String>();
        String line;

        try {
            BufferedReader buffReader = new BufferedReader(new FileReader(dataPath));
            do {
                line = buffReader.readLine();
                lineBuffer.add(line);
            }
            while(line != null);
        }catch(FileNotFoundException e) {
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
        this.lineBufferItr = lineBuffer.iterator();

    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {

        Random random = new Random();
        int millis = random.nextInt(30) + 10;
        Utils.sleep(millis);
        String[] lineArray = new String[]{"NA", "NA", "NA", "NA", "NA", "NA"};

        if(lineBufferItr.hasNext()) {
            String currentLine = (String)this.lineBufferItr.next();
            try {
                lineArray = currentLine.split(",");
                // Format: 0 - polarity, 1 - id, 2 - date, 3 - query, 4 - user, 5 - text

            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        this.collector.emit(new Values(lineArray[1], lineArray[2], lineArray[5]));

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet-id", "date", "text"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
