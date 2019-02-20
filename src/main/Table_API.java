package main;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class Table_API {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //      Get the Table environment for table APIs

        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(env);

        //      Read data from csv to populate in Table

        CsvTableSource dataSource = new CsvTableSource.Builder()
                .ignoreFirstLine()
                .path("src/ml-latest-small/movies.csv")
                .field("id", Types.INT())
                .field("name", Types.STRING())
                .field("genre", Types.STRING())
                .build();


        //      Register Table as data source

        tableEnvironment.registerTableSource("Movies", dataSource);

        TableSink csvSink = new CsvTableSink("src/ml-latest-small/sink.csv", "|", 1, FileSystem.WriteMode.OVERWRITE);

        String[] fileds = {"id", "name", "genre"};
        TypeInformation[] fieldTypes = {Types.INT(), Types.STRING(), Types.STRING()};

        tableEnvironment.registerTableSink("Sink", fileds, fieldTypes, csvSink);


        //          Transformation is being applied
        //

        Table scanedResults = tableEnvironment.scan("Movies").where("id=6").select("id, name, genre");
//        scanedResults.printSchema();


        env.setParallelism(1);


        DataSet<Row> result = tableEnvironment.toDataSet(scanedResults, Row.class);

        // we  require one of the below statement
        scanedResults.insertInto("Sink");
//        scanedResults.writeToSink(csvSink);
//        result.writeAsText("src/ml-latest-small/sink.csv", FileSystem.WriteMode.OVERWRITE);
        result.print();
    }

}