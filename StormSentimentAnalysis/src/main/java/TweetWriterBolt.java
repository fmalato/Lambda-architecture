import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
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

public class TweetWriterBolt implements IRichBolt {

    private OutputCollector collector;
    private BufferedWriter writer;
    private Configuration configuration;
    private Table table;

    /*** The prepare() method of this class initialized the needed HBase objects. The HTableDescriptor object is in
     * charge of the table setup, which name is given by the queryString variable. After that, the HBaseAdmin creates
     * the table.
     */

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        try {
            this.writer = new BufferedWriter(new FileWriter("db.txt"));
        } catch(IOException e) {
            e.printStackTrace();
        }

        try {
            configuration = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configuration);
            Admin admin  = connection.getAdmin();

            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TweetTopology.queryString));
            tableDescriptor.addFamily(new HColumnDescriptor("number"));

            if(!admin.tableExists(TableName.valueOf(TweetTopology.queryString))) {
                admin.createTable(tableDescriptor);
            }
            table = connection.getTable(TableName.valueOf(TweetTopology.queryString));

        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /*** Based on the score computed from the TweetClassifierBolt, this method gives the correct sentiment and
     * updates the HBase table.
     */

    @Override
    public void execute(Tuple tuple) {

        Integer score = (Integer)tuple.getValueByField("score");
        String record = score.toString();

        String sentiment = "Neutral";
        if(score > 0 && score <= 3) {
            sentiment = "Positive";
        }
        else if(score > 3) {
            sentiment = "VeryPositive";
        }
        else if(score < 0 && score >= -3) {
            sentiment = "Negative";
        }
        else if(score < -3) {
            sentiment = "VeryNegative";
        }

        try {
            this.writer.write(record + " - " + sentiment + "\n");
            this.writer.flush();

        } catch(IOException e) {
            e.printStackTrace();
        }

        Get g = new Get(Bytes.toBytes(sentiment));
        try {
            Result resultTable = table.get(g);
            byte[] oldByteValue = resultTable.getValue(Bytes.toBytes("number"), Bytes.toBytes("value"));

            int oldValue = 1;
            if(Bytes.toString(oldByteValue) != null) {
                oldValue = Integer.parseInt(Bytes.toString(oldByteValue));
                oldValue++;
            }

            Put put = new Put(Bytes.toBytes(sentiment));
            String result = Integer.toString(oldValue);
            put.addColumn(Bytes.toBytes("number"), Bytes.toBytes("value"), Bytes.toBytes(result));

            Table table = ConnectionFactory.createConnection(configuration)
                    .getTable(TableName.valueOf(TweetTopology.queryString));
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cleanup() { }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("query", "score"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}