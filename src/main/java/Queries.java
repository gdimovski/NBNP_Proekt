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

    public static void getSingleRowByKey(Table table, String key, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Get get = new Get(Bytes.toBytes(key));
        Result result = table.get(get);
        printResult(result);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get single row by key' is: " + duration + " milliseconds", 1d);
    }

    public static void getAllRowsInTheTable(Table table, HashMap<String,Double> map) throws IOException
    {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int counter = 1;
        for (Result r : scanner) {
            counter++;
            printResult(r);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get get all rows in the table' is: " + duration + " milliseconds", Double.valueOf(counter));
    }

    public static void getAllRowsWithASpecificValueInAColumn(Table table, String column, String value, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(column), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value)));
        ResultScanner scanner = table.getScanner(scan);
        int count = 1;
        for (Result r : scanner) {
            count++;
            printResult(r);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with a specific value in a column' is: " + duration + " milliseconds", Double.valueOf(count));
    }

    public static void getAllRowsWithValuesInASpecifiedRangeInAColumn(Table table, String column, String lowerValue, String upperValue, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(lowerValue)));
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperValue)));
        ResultScanner scanner = table.getScanner(scan);
        int count = 1;
        for (Result r : scanner) {
            count++;
            printResult(r);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in a column' is: " + duration + " milliseconds", Double.valueOf(count));
    }

    public static void getAverageValueOfColumn(Table table, String column, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        double total = 0;
        for (Result r : scanner) {
            count++;
            total += Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(column))));
        }
        double avg = total / count;
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the average value of a column' is: " + duration + " milliseconds", avg);
    }

    public static void getMaximumValueOfAColumn(Table table, String column, HashMap<String, Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        double max = Double.MIN_VALUE;
        for (Result r : scanner) {
            double value = Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(column))));
            if (value > max) {
                max = value;
            }
        }
        System.out.println("Max value of" + column + " is " + max);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the maximum value of a column' is: " + duration + " milliseconds", max);

    }

    public static void getMinimumValueOfAColumn(Table table, String column, HashMap<String, Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        double min = Double.MAX_VALUE;
        for (Result r : scanner) {
            double value = Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(column))));
            if (value < min) {
                min = value;
            }
        }
        System.out.println("Min value of " +  column + " is " + min);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the minimum value of a column' is: " + duration + " milliseconds", min);
    }

    public static void getCountOfRowsWithSpecificValueInAColumn(Table table, String column, String value, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(column), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(value)));
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        for (Result r : scanner) {
            count++;
        }
        System.out.println("Count of values measured at " + column + " " + value + " is "+ count);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the count of rows with a specific value in a column' is: " + duration + " milliseconds", Double.valueOf(count));
    }

    public static void getSumOfValuesInColumn(Table table, String column, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        double sum = 0;
        for (Result r : scanner) {
            sum += Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes(column))));
        }
        System.out.println("The sum of values of "+ column + " is " + sum);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get the sum of values in a column' is: " + duration + " milliseconds", Double.valueOf(sum));
    }

    public static void getAllRowsWithValuesInASpecifiedRangeInMultipleColumns(Table table, String firstColumn, String firstLowerValue, String firstUpperValue, String secondColumn, String secondLowerValue, String secondUpperValue, HashMap<String, Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        FilterList filters = new FilterList();
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(firstColumn), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(firstLowerValue)));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(firstColumn), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(firstUpperValue)));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(secondColumn), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(secondLowerValue)));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(secondColumn), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(secondUpperValue)));
        scan.setFilter(filters);
        ResultScanner scanner = table.getScanner(scan);
        int count = 1;
        for (Result r : scanner) {
            count++;
            printResult(r);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in multiple columns: " + duration + " milliseconds", Double.valueOf(count));
    }

    public static void getAllRowsWithValuesInASpecifiedRangeInMultipleColumnsAndExcludeRowsWithSpecificValueInAnotherColumn(Table table, String firstColumn, String firstLowerValue, String firstUpperValue, String secondColumn, String secondLowerValue, String secondUpperValue, String excludeColumn, String excludeValue, HashMap<String, Double> map) throws IOException
    {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(firstColumn), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(firstLowerValue)));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(firstColumn), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(firstUpperValue)));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(secondColumn), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(secondLowerValue)));
        filters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(secondColumn), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(secondUpperValue)));
        filters.addFilter(new SingleColumnValueExcludeFilter(Bytes.toBytes("data"), Bytes.toBytes(excludeColumn), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(excludeValue)));
        scan.setFilter(filters);
        ResultScanner scanner = table.getScanner(scan);
        int count = 1;
        for (Result r : scanner) {
            count++;
            printResult(r);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in multiple columns and exclude rows with a specific value in another column' is: " + duration + " milliseconds", Double.valueOf(count));
    }

    public static void getAllRowsWithValuesInASpecifiedRangeInASingleColumnAndReturnOnlySpecificColumns(Table table, String column, String lowerValue, String upperValue, String firstSelectColumn, String secondSelectColumn, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(lowerValue)));
        scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes("data"), Bytes.toBytes(column), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(upperValue)));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(firstSelectColumn));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes(secondSelectColumn));
        ResultScanner scanner = table.getScanner(scan);
        int count = 1;
        for (Result r : scanner) {
            count++;
            printResult(r);
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get all rows with values in a specified range in a single column and return only specific columns' is: " + duration + " milliseconds", Double.valueOf(count));
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("air_pollution"));

        HashMap<String, Double> map = new HashMap<>();

        getSingleRowByKey(table, "2017-09-30 20:00", map);

        getAllRowsInTheTable(table,map);

        getAllRowsWithASpecificValueInAColumn(table, "Station code", "125", map);

        getAllRowsWithValuesInASpecifiedRangeInAColumn(table, "SO2", "0", "0,1", map);

        getAverageValueOfColumn(table, "SO2", map);

        getMaximumValueOfAColumn(table, "SO2", map);

        getMinimumValueOfAColumn(table, "SO2", map);

        getCountOfRowsWithSpecificValueInAColumn(table, "SO2", "125", map);

        getSumOfValuesInColumn(table, "SO2", map);

        getAllRowsWithValuesInASpecifiedRangeInMultipleColumns(table, "SO2", "0", "0.1", "PM10", "0", "100", map);

        getAllRowsWithValuesInASpecifiedRangeInMultipleColumnsAndExcludeRowsWithSpecificValueInAnotherColumn(table, "SO2", "0", "0.1", "PM10", "0", "100", "Station code", "125", map);

        getAllRowsWithValuesInASpecifiedRangeInASingleColumnAndReturnOnlySpecificColumns(table, "SO2", "0", "0.1", "date", "Station code", map);

        Set<String> keys = map.keySet();
        for(String key: keys){
            double value = map.get(key);
            System.out.println(key + " with number of result: " + value);
        }
        table.close();
        connection.close();
    }
}
