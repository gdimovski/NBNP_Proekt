import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class DataOperations {
    public static void main(String[] args) throws IOException {
        fillHbaseDatatable();
        System.out.println("Data inserted successfully");
    }

    public static List<CSVRecord> getCsvRecords() {
        List<CSVRecord> csvRecords = new ArrayList<>();
        String fileName = "src\\main\\java\\newSummary.csv";

        try (FileReader reader = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(reader);
            for (CSVRecord record : records) {
                csvRecords.add(record);
            }
        } catch (IOException e) {
            System.out.println("An error occurred while reading the file: " + e.getMessage());
        }
        return csvRecords;
    }

    public static void fillHbaseDatatable() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("air_pollution"));
        List<CSVRecord> records = getCsvRecords();

        int i = 1;
        // Load data into HBase [Measurement date, Station code, Address, Latitude, Longitude, SO2, NO2, O3, CO, PM10, PM2.5]
        for (CSVRecord record : records) {
            System.out.println("Putting "+ record.get("Measurement date") + " that is record number: " + i++);
            Put put = new Put(Bytes.toBytes(record.get("Measurement date")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Station code"), Bytes.toBytes(record.get("Station code")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Address"), Bytes.toBytes(record.get("Address")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Latitude"), Bytes.toBytes(record.get("Latitude")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("Longitude"), Bytes.toBytes(record.get("Longitude")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("SO2"), Bytes.toBytes(record.get("SO2")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("NO2"), Bytes.toBytes(record.get("NO2")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("O3"), Bytes.toBytes(record.get("O3")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("CO"), Bytes.toBytes(record.get("CO")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM10"), Bytes.toBytes(record.get("PM10")));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("PM2.5"), Bytes.toBytes(record.get("PM2.5")));
            table.put(put);
        }

        table.close();
        connection.close();
    }
}
