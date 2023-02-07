import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;


public class Queries {

    public static void printResult(Result result){
        System.out.println("Result is ");
        System.out.println("Row Key: " + Bytes.toString(result.getRow()));
        for (Cell cell : result.listCells()) {
            System.out.println(Bytes.toString(cell.getValueArray()));
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("air_pollution"));

        HashMap<String, Double> map = new HashMap<>();

        long startTime = System.currentTimeMillis();

        // Get a single row by key
        Get get = new Get(Bytes.toBytes("2017-09-30 20:00"));
        Result result = table.get(get);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        printResult(result);
        map.put("Time taken to execute the query 'Get single row by key' is: " + duration + " milliseconds", 1d);
        System.out.println("Time taken to execute the query 'Get single row by key' is: " + duration + " milliseconds");

        startTime = System.currentTimeMillis();

        // Get all rows in the table
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int allRowsInTableCounter = 1;
        for (Result r : scanner) {
            // process each result
            System.out.println(allRowsInTableCounter++);
            printResult(r);
        }

        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get get all rows in the table' is: " + duration + " milliseconds", Double.valueOf(allRowsInTableCounter));
        System.out.println("Time taken to execute the query 'Get get all rows in the table' is: " + duration + " milliseconds");


        startTime = System.currentTimeMillis();

        // Get all rows with a specific value in a column:
        Scan scan_1 = new Scan();
        scan_1.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("Station code"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("125")));
        ResultScanner scanner_1 = table.getScanner(scan_1);
        int allRowsWithASpecificValueCounter = 1;
        for (Result r : scanner_1) {
            // process each result
            System.out.println(allRowsWithASpecificValueCounter++);
            printResult(r);
        }

        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with a specific value in a column' is: " + duration + " milliseconds", Double.valueOf(allRowsWithASpecificValueCounter));
        System.out.println("Time taken to execute the query 'Get all rows with a specific value in a column' is: " + duration + " milliseconds");


        startTime = System.currentTimeMillis();

        // Get all rows with values in a specified range in a column:
        Scan scan_2 = new Scan();
        scan_2.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0")));
        scan_2.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("0.1")));
        ResultScanner scanner_2 = table.getScanner(scan_2);
        int getAllRowsWithValuesInARange = 1;
        for (Result r : scanner_2) {
            // process each result
            System.out.println(getAllRowsWithValuesInARange++);
            printResult(r);
        }

        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in a column' is: " + duration + " milliseconds", Double.valueOf(getAllRowsWithValuesInARange));
        System.out.println("Time taken to execute the query 'Get all rows with values in a specified range in a column' is: " + duration + " milliseconds");


        startTime = System.currentTimeMillis();

        // Get the average value of a column:
        Scan scan_3 = new Scan();
        ResultScanner scanner_3 = table.getScanner(scan_3);
        int count_3 = 0;
        double total_3 = 0;
        for (Result r : scanner_3) {
            count_3++;
            total_3 += Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("SO2"))));
        }
        double avg_3 = total_3 / count_3;
        System.out.println("Average value of SO2 is " +avg_3);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the average value of a column' is: " + duration + " milliseconds", avg_3);
        System.out.println("Time taken to execute the query 'Get the average value of a column' is: " + duration + " milliseconds");


        startTime = System.currentTimeMillis();

        // Get the maximum value of a column:
        Scan scan_4 = new Scan();
        ResultScanner scanner_4 = table.getScanner(scan_4);
        double max_4 = Double.MIN_VALUE;
        for (Result r : scanner_4) {
            double value_4 = Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("SO2"))));
            if (value_4 > max_4) {
                max_4 = value_4;
            }
        }
        System.out.println("Max value of S02 is "+ max_4);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get get the maximum value of a column' is: " + duration + " milliseconds", max_4);
        System.out.println("Time taken to execute the query 'Get get the maximum value of a column' is: " + duration + " milliseconds");


        startTime = System.currentTimeMillis();

        // Get the minimum value of a column:
        Scan scan_5 = new Scan();
        ResultScanner scanner_5 = table.getScanner(scan_5);
        double min_5 = Double.MAX_VALUE;
        for (Result r : scanner_5) {
            double value_5 = Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("SO2"))));
            if (value_5 < min_5) {
                min_5 = value_5;
            }
        }

        System.out.println("Min value of S02 is " + min_5);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the minimum value of a column' is: " + duration + " milliseconds", min_5);
        System.out.println("Time taken to execute the query 'Get the minimum value of a column' is: " + duration + " milliseconds");



        startTime = System.currentTimeMillis();

        // Get the count of rows with a specific value in a column:
        Scan scan_6 = new Scan();
        scan_6.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("Station code"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("125")));
        ResultScanner scanner_6 = table.getScanner(scan_6);
        int count_6 = 0;
        for (Result r : scanner_6) {
            count_6++;
        }

        System.out.println("Count of values measured at Station code 125 is "+ count_6);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the count of rows with a specific value in a column' is: " + duration + " milliseconds", Double.valueOf(count_6));
        System.out.println("Time taken to execute the query 'Get the count of rows with a specific value in a column' is: " + duration + " milliseconds");



        startTime = System.currentTimeMillis();

        // Get the sum of values in a column:
        Scan scan_7 = new Scan();
        ResultScanner scanner_7 = table.getScanner(scan_7);
        double sum_7 = 0;
        for (Result r : scanner_7) {
            sum_7 += Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("SO2"))));
        }
        System.out.println("The sum of values of S02 is " + sum_7);
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the sum of values in a column' is: " + duration + " milliseconds", Double.valueOf(sum_7));
        System.out.println("Time taken to execute the query 'Get the sum of values in a column' is: " + duration + " milliseconds");



        startTime = System.currentTimeMillis();

        // Get all rows with values in a specified range in multiple columns:
        Scan scan_8 = new Scan();
        FilterList filters_8 = new FilterList();
        filters_8.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0")));
        filters_8.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("0.1")));
        filters_8.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("PM10"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0")));
        filters_8.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("PM10"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("100")));
        scan_8.setFilter(filters_8);
        ResultScanner scanner_8 = table.getScanner(scan_8);
        int countOfInMultipleColumnRange = 1;
        for (Result r : scanner_8) {
            // process each result
            System.out.println(countOfInMultipleColumnRange++);
            printResult(r);
        }

        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in multiple columns: " + duration + " milliseconds", Double.valueOf(countOfInMultipleColumnRange));
        System.out.println("Time taken to execute the query 'Get all rows with values in a specified range in multiple columns: " + duration + " milliseconds");




        startTime = System.currentTimeMillis();
        // Get all rows with values in a specified range in multiple columns and exclude rows with a specific value in another column:
        Scan scan_9 = new Scan();
        FilterList filters_9 = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filters_9.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0")));
        filters_9.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("0.1")));
        filters_9.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("PM10"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0")));
        filters_9.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("PM10"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("100")));
        filters_9.addFilter(new SingleColumnValueExcludeFilter(Bytes.toBytes("data"), Bytes.toBytes("Station code"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("125")));
        scan_9.setFilter(filters_9);
        ResultScanner scanner_9 = table.getScanner(scan_9);
        int countOfInMultipleColumnRangeWithExclusionOfStation = 1;
        for (Result r : scanner_9) {
            System.out.println(countOfInMultipleColumnRangeWithExclusionOfStation++);
            // process each result
            printResult(r);
        }
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in multiple columns and exclude rows with a specific value in another column' is: " + duration + " milliseconds", Double.valueOf(countOfInMultipleColumnRangeWithExclusionOfStation));
        System.out.println("Time taken to execute the query 'Get all rows with values in a specified range in multiple columns and exclude rows with a specific value in another column' is: " + duration + " milliseconds");



        startTime = System.currentTimeMillis();
        // Get all rows with values in a specified range in a single column and return only specific columns:
        Scan scan_10 = new Scan();
        scan_10.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0")));
        scan_10.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("SO2"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("0.1")));
        scan_10.addColumn(Bytes.toBytes("data"), Bytes.toBytes("date"));
        scan_10.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Station code"));
        ResultScanner scanner_10 = table.getScanner(scan_10);
        int countOfInSpecifiedRangeWithResultWithSpecificColumns = 1;
        for (Result r : scanner_10) {
            System.out.println(countOfInSpecifiedRangeWithResultWithSpecificColumns++);
            // process each result
            printResult(r);

        }
        endTime = System.currentTimeMillis();
        duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in a single column and return only specific columns' is: " + duration + " milliseconds", Double.valueOf(countOfInSpecifiedRangeWithResultWithSpecificColumns));
        System.out.println("Time taken to execute the query 'Get all rows with values in a specified range in a single column and return only specific columns' is: " + duration + " milliseconds");



//        //  Get all rows with values in a specified range in multiple columns, sort the results by the value in a specific column, and return only specific columns:
//        Scan scan_11 = new Scan();
//        FilterList filters_11 = new FilterList();
//        filters_11.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("so2"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("<min_so2>")));
//        filters_11.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("so2"), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes("<max_so2>")));
//        filters_11.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("pm10"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("<min_pm10>")));
//        filters_11.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes("pm10"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("<max_pm10>")));
//        scan_11.setFilter(filters_11);
//        scan_11.addColumn(Bytes.toBytes("data"), Bytes.toBytes("date"));
//        scan_11.addColumn(Bytes.toBytes("data"), Bytes.toBytes("site"));
//        scan_11.setSortOrder(new SortOrder(Bytes.toBytes("data"), Bytes.toBytes("date"), false));
//        ResultScanner scanner_11 = table.getScanner(scan_11);
//        for (Result r : scanner_11) {
//            // process each result
//        }

        Set<String> keys = map.keySet();
        for(String key: keys){
            double value = map.get(key);
            System.out.println(key + " with number of result: " + value);
        }
        table.close();
        connection.close();
    }
}
