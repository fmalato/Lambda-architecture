import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
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

        if(lineBufferItr.hasNext()) {
            String currentLine = (String)this.lineBufferItr.next();
            try {
                String[] lineArray = currentLine.split(",");
                Tweet tweet = new Tweet(lineArray[0], lineArray[1], lineArray[2]);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("text", "date", "timestamp"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
