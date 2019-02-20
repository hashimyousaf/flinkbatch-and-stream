package main.UDF;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import scala.reflect.internal.util.TableDef;

import java.util.Iterator;

public class TableApiUdfAggregate {

    public static void main(String[] args) throws Exception {
        System.out.println("This is hashim yousaf");
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.getTableEnvironment(env);

        // register the function
        tableEnvironment.registerFunction("wAvg", new WeightedAvg());

        // create a TableSource
        CsvTableSource dataSource = new CsvTableSource.Builder()
                .ignoreFirstLine()
                .path("src/ml-latest-small/ratings.csv")
                .field("userId", Types.INT())
                .field("movieId", Types.INT())
                .field("rating", Types.DOUBLE())
                .build();
        // register the TableSource as table "CsvTable"
        tableEnvironment.registerTableSource("ratings", dataSource);

//        tableEnvironment.sqlQuery("SELECT userId, wAvg(rating) AS avgPoints FROM ratings GROUP BY userId");
        Table scanedResults = tableEnvironment.sqlQuery("SELECT userId, wAvg(rating) FROM ratings GROUP BY userId");

        DataSet<Row> result  = tableEnvironment.toDataSet( scanedResults, Row.class);
        result.print();

//        DataSet<Row> data  = tableEnvironment.toDataSet(table, Row.class);
//        data.print();

    }

    /**
     * Accumulator for WeightedAvg.
     */
    public static class WeightedAvgAccum {
        public Double sum = 0.0;
        public int count = 0;
    }

    /**
     * Weighted Average user-defined aggregate function.
     */
    public static class WeightedAvg extends AggregateFunction<Double, WeightedAvgAccum> {

        @Override
        public WeightedAvgAccum createAccumulator() {
            return new WeightedAvgAccum();
        }

        @Override
        public Double getValue(WeightedAvgAccum acc) {
            if (acc.count == 0) {
                return null;
            } else {
                return acc.sum / acc.count;
            }
        }

        public void accumulate(WeightedAvgAccum acc, Double iValue, int iWeight) {
            acc.sum += iValue * iWeight;
            acc.count += iWeight;
        }

        public void retract(WeightedAvgAccum acc, Double iValue, int iWeight) {
            acc.sum -= iValue * iWeight;
            acc.count -= iWeight;
        }

        public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
            Iterator<WeightedAvgAccum> iter = it.iterator();
            while (iter.hasNext()) {
                WeightedAvgAccum a = iter.next();
                acc.count += a.count;
                acc.sum += a.sum;
            }
        }

        public void resetAccumulator(WeightedAvgAccum acc) {
            acc.count = 0;
            acc.sum = 0D;
        }
    }

}

