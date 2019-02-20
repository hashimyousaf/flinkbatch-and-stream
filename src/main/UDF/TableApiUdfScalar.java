package main.UDF;

import HelperClasses.HashCode;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class TableApiUdfScalar {
    public static void main(String[] args) throws Exception{
        System.out.println("This is hashim yousaf");

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(env);

        // register the function
        tableEnvironment.registerFunction("hashCode", new HashCode(10));

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

// use the function in Java Table API
//        myTable.select("string, string.hashCode(), hashCode(string)");
        // scan registered Orders table
        Table orders = tableEnvironment.scan("short-movies").select ("id, name.hashCode(), hashCode(genre)");

        DataSet<Row> data  = tableEnvironment.toDataSet(orders, Row.class);

        data.print();
// use the function in SQL API
//        tableEnvironment.sqlQuery("SELECT string, HASHCODE(string) FROM MyTable");


    }
}

