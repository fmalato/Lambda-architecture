import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.io.IOException;
import java.util.ArrayList;

public class SentimentBarChart extends ApplicationFrame {

    public SentimentBarChart(String applicationTitle , String chartTitle) throws IOException {
        super( applicationTitle );
        JFreeChart barChart = ChartFactory.createBarChart(
                chartTitle,
                "Sentiment",
                "Count",
                createDataset(),
                PlotOrientation.VERTICAL,
                true, true, false);

        ChartPanel chartPanel = new ChartPanel(barChart);
        chartPanel.setPreferredSize(new java.awt.Dimension( 400 , 350 ) );
        setContentPane(chartPanel);
    }

    private CategoryDataset createDataset() throws IOException {

        DefaultCategoryDataset dataset = new DefaultCategoryDataset( );

        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin  = connection.getAdmin();

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(queryString));
        tableDescriptor.addFamily(new HColumnDescriptor("number"));

        if(!admin.tableExists(TableName.valueOf(queryString))) {
            admin.createTable(tableDescriptor);
        }

        Table table = connection.getTable(TableName.valueOf(queryString));

        try {
            for (String sentiment : sentiments) {
                Get g = new Get(Bytes.toBytes(sentiment));

                Result resultTable = table.get(g);
                byte[] oldByteValue = resultTable.getValue(Bytes.toBytes("number"), Bytes.toBytes("value"));

                int value = 0;
                if (Bytes.toString(oldByteValue) != null) {
                    value = Integer.parseInt(Bytes.toString(oldByteValue));
                }

                dataset.addValue(value, sentiment, "");
            }
        } finally {
            table.close();
            connection.close();
        }

        return dataset;
    }

    private static String queryString = null;
    private static ArrayList<String> sentiments = new ArrayList<String>();

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length > 0)
            queryString = args[0];

        if (queryString == null) {
            System.out.println("Please insert a query word");
            return;
        }

        sentiments.add("VeryNegative");
        sentiments.add("Negative");
        sentiments.add("Neutral");
        sentiments.add("Positive");
        sentiments.add("VeryPositive");

        SentimentBarChart chart = new SentimentBarChart("Twitter Sentiment Analysis",
                "Query: " + queryString);
        chart.pack();
        RefineryUtilities.centerFrameOnScreen(chart);
        chart.setVisible(true);

        while (true) {
            Thread.sleep(10000);

            JFreeChart barChart = ChartFactory.createBarChart(
                    "Query: " + queryString,
                    "Sentiment",
                    "Count",
                    chart.createDataset(),
                    PlotOrientation.VERTICAL,
                    true, true, false);

            ChartPanel chartPanel = new ChartPanel(barChart);
            chartPanel.setPreferredSize(new java.awt.Dimension( 400 , 350 ) );
            chart.setContentPane(chartPanel);

            chart.pack();
        }

    }

}
