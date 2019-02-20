package main.UDF;

import HelperClasses.HashCode;
import HelperClasses.Split;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class TableApiUdfTableFunction {
    public static void main(String[] args) throws Exception {
        System.out.println("This is hashim yousaf");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(env);

        // register the function
        tableEnvironment.registerFunction("split", new Split("\\|"));

        // create a TableSource
        CsvTableSource dataSource = new CsvTableSource.Builder()
                .ignoreFirstLine()
                .path("src/ml-latest-small/short-movies.csv")
                .field("id", Types.INT())
                .field("name", Types.STRING())
                .field("genre", Types.STRING())
                .build();
        // register the TableSource as table "CsvTable"
        tableEnvironment.registerTableSource("short-movies", dataSource);

        Table movies = tableEnvironment.scan("short-movies").select ("id, name, genre");


        Table table = movies.join(new Table(tableEnvironment, "split(genre) as (word, length)"))
                .select("id, name, genre, length");

        DataSet<Row> data  = tableEnvironment.toDataSet(table, Row.class);
        data.print();

    }
}
