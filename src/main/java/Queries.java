import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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

    public static void getPM_MeasurementsInMonth(Table table, String month, HashMap<String,Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("data"),
                Bytes.toBytes("Measurement date"),
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("^*-"+month+"-*$")
        );
        scan.setFilter(filter);

        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM10"));

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            byte[] pm25 = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"));
            byte[] pm10 = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM10"));

            System.out.println("Measurement date: " + Bytes.toString(result.getRow()));
            System.out.println("PM2.5: " + Float.valueOf(Bytes.toString(pm25)));
            System.out.println("PM10: " + Float.valueOf(Bytes.toString(pm10)));
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'PM measurements in "+month+" month' is: " + duration + " milliseconds", 1d);
    }

    public static void getTimeAndStationWithHighestAverageValueOfPollution(Table table, HashMap<String, Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Station code"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("SO2"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("NO2"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("CO"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("O3"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM10"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"));

        ResultScanner resultScanner = table.getScanner(scan);

        int stationCode = 0;
        String date = "";
        float maxAvg = 0;

        for (Result result : resultScanner) {
            String currentDate = Bytes.toString(result.getRow());
            int currentStationCode = Integer.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("Station code"))));
            float so2 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("SO2"))));
            float no2 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("NO2"))));
            float co = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("CO"))));
            float o3 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("O3"))));
            float pm10 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM10"))));
            float pm25 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"))));

            float avg = (so2 + no2 + co + o3 + pm10 + pm25) / 6;
            if (avg > maxAvg) {
                maxAvg = avg;
                stationCode = currentStationCode;
                date = currentDate;
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get station with highest average value of pollution' is: " + duration + " milliseconds and date is: "+date, Double.valueOf(stationCode));
    }

    public static void getTimeAndStationWithLowestAverageValueOfPollution(Table table, HashMap<String, Double> map) throws IOException {
        long startTime = System.currentTimeMillis();
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Station code"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("SO2"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("NO2"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("CO"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("O3"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM10"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"));

        ResultScanner resultScanner = table.getScanner(scan);

        int stationCode = 0;
        String date = "";
        float minAvg = Float.MAX_VALUE;

        for (Result result : resultScanner) {
            String currentDate = Bytes.toString(result.getRow());
            int currentStationCode = Integer.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("Station code"))));
            float so2 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("SO2"))));
            float no2 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("NO2"))));
            float co = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("CO"))));
            float o3 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("O3"))));
            float pm10 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM10"))));
            float pm25 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"))));

            float avg = (so2 + no2 + co + o3 + pm10 + pm25) / 6;
            if (avg < minAvg) {
                minAvg = avg;
                stationCode = currentStationCode;
                date = currentDate;
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get station with lowest average value of pollution' is: " + duration + " milliseconds and date is: "+date, Double.valueOf(stationCode));
    }

    public static void getMonthWithHighestAveragePollution(Table table, HashMap<String,Double> map) throws IOException, ParseException {
        long startTime = System.currentTimeMillis();
        HashMap<String, Float> avgPollutionPerMonth = new HashMap<>();
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("data"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            String date = Bytes.toString(result.getRow());
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            Date measurementDate = dateFormat.parse(date);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(measurementDate);
            String month = new SimpleDateFormat("MMMM").format(calendar.getTime());
            float so2 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("SO2"))));
            float no2 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("NO2"))));
            float co = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("CO"))));
            float o3 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("O3"))));
            float pm10 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM10"))));
            float pm25 = Float.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"))));
            float avgPollution = (so2 + no2 + co + o3 + pm10 + pm25) / 6;
            if (!avgPollutionPerMonth.containsKey(month)) {
                avgPollutionPerMonth.put(month, avgPollution);
            } else {
                float currentAvgPollution = avgPollutionPerMonth.get(month);
                avgPollutionPerMonth.put(month, currentAvgPollution + avgPollution);
            }
        }
        scanner.close();
        double maxAvgPollution = Double.MIN_VALUE;
        String monthWithHighestAvgPollution = "";
        for (HashMap.Entry<String, Float> entry : avgPollutionPerMonth.entrySet()) {
            double avgPollution = entry.getValue() / 645000;
            if (avgPollution > maxAvgPollution) {
                maxAvgPollution = avgPollution;
                monthWithHighestAvgPollution = entry.getKey();
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        map.put("Time taken to execute the query 'Get month with highest average pollution' is: " + duration + " milliseconds is month:"+monthWithHighestAvgPollution, Double.valueOf(maxAvgPollution));
    }


    public static void main(String[] args) throws IOException, ParseException {
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

        getPM_MeasurementsInMonth(table, "12", map);

        getTimeAndStationWithHighestAverageValueOfPollution(table, map);

        getTimeAndStationWithLowestAverageValueOfPollution(table, map);

        getMonthWithHighestAveragePollution(table,map);

        Set<String> keys = map.keySet();
        for(String key: keys){
            double value = map.get(key);
            System.out.println(key + " with number of result: " + value);
        }
        table.close();
        connection.close();
    }
}
