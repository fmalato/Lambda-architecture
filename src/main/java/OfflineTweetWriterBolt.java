import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class OfflineTweetWriterBolt implements IRichBolt {

    private OutputCollector collector;
    private BufferedWriter writer;
    private Configuration configuration;
    private HTable hTable;
    private String queryString = "Trump";

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        try {
            this.writer = new BufferedWriter(new FileWriter("db.txt"));
        } catch(IOException e) {
            e.printStackTrace();
        }

        try {
            configuration = new HBaseConfiguration();
            HBaseAdmin admin = new HBaseAdmin(configuration);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(queryString));
            tableDescriptor.addFamily(new HColumnDescriptor("number"));

            if(!admin.tableExists(TableName.valueOf(queryString))) {
                admin.createTable(tableDescriptor);
            }
            hTable = new HTable(configuration, queryString);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void execute(Tuple tuple) {

        Integer score = (Integer)tuple.getValueByField("score");
        String record = score.toString();

        int veryPositive = 0;
        int positive = 0;
        int neutral = 0;
        int negative = 0;
        int veryNegative = 0;

        String sentiment = "Neutral";
        if(score > 0 && score <= 3) {
            sentiment = "Positive";
            positive++;
        }
        else if(score > 3) {
            sentiment = " VeryPositive";
            veryPositive++;
        }
        else if(score < 0 && score >= -3) {
            sentiment = "Negative";
            negative++;
        }
        else if(score < -3) {
            sentiment = "VeryNegative";
            veryNegative++;
        }
        else {
            neutral++;
        }

        try {
            this.writer.write(record + " - " + sentiment + "\n");
            this.writer.flush();

        } catch(IOException e) {
            e.printStackTrace();
        }

        Get g = new Get(Bytes.toBytes(sentiment));
        try {
            Result resultTable = hTable.get(g);
            byte[] oldByteValue = resultTable.getValue(Bytes.toBytes("number"), Bytes.toBytes("value"));

            int oldValue = 1;
            if(Bytes.toString(oldByteValue) != null) {
                oldValue = Integer.parseInt(Bytes.toString(oldByteValue));
                oldValue++;
            }

            Put put = new Put(Bytes.toBytes(sentiment));
            String result = Integer.toString(oldValue);
            put.addColumn(Bytes.toBytes("number"), Bytes.toBytes("value"), Bytes.toBytes(result));

            Table table = ConnectionFactory.createConnection(configuration).getTable(TableName.valueOf(queryString));
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("query", "score"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
