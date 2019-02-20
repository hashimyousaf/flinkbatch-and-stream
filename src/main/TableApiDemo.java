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

public class TableApiDemo {
    public static void main(String[] args) throws Exception {
        System.out.println("This is hashim yousaf");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // get a batch table environment
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(env);

        // create a TableSource
        String[] fieldNames = {"id", "name", "genre"};
        TypeInformation[] fieldTypes = {Types.INT(), Types.STRING(), Types.STRING()};
        CsvTableSource csvSource = new CsvTableSource("src/ml-latest-small/movies.csv",fieldNames,fieldTypes);


        System.out.println(csvSource);

        // register the TableSource as table "CsvTable"
        tableEnvironment.registerTableSource("movies", csvSource);

        //======================= Register a table sink =======================
        // create a TableSink
        TableSink csvSink = new CsvTableSink("src/ml-latest-small/sink.csv",",",1, FileSystem.WriteMode.OVERWRITE);

        // define the field names and types
        String[] resultFieldNames = {"id", "movie", "genre"};
        TypeInformation[] resultFieldTypes = {Types.INT(), Types.STRING(), Types.STRING()};

        // register the TableSink as table "CsvSinkTable"
        tableEnvironment.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink);

        // scan registered Orders table
        Table orders = tableEnvironment.scan("movies").select ("id, name, genre");
        // compute revenue for all customers from France

        env.setParallelism(1);

        DataSet<Row> data  = tableEnvironment.toDataSet(orders, Row.class);
        data.writeAsText("src/ml-latest-small/sink.csv", FileSystem.WriteMode.OVERWRITE);
        data.print();

//        System.out.println(revenue);
    }
}
