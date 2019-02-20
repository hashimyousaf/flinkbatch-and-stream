import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class GroupReduceFn {

    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple2<Integer, String>> input = env.readCsvFile("src/ml-latest-small/short-movies.csv")
                .ignoreFirstLine()
                .includeFields(true, true, false)
                .types(Integer.class, String.class); // movieId and title



        DataSet<Tuple2<Integer, String>> output = input
                .groupBy(0)            // group DataSet by the first tuple field
                .reduceGroup(new DistinctReduce())
                .sortPartition(0, Order.ASCENDING);  // apply GroupReduceFunction
        input.print();
    }
}

class DistinctReduce
        implements GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>> {

    public void reduce(Iterable<Tuple2<Integer, String>> in, Collector<Tuple2<Integer, String>> out) {

        Set<String> uniqStrings = new HashSet<String>();
        Integer key = null;

        // add all strings of the group to the set
        for (Tuple2<Integer, String> t : in) {
            key = t.f0;
            uniqStrings.add(t.f1);
        }

        // emit all unique strings.
        for (String s : uniqStrings) {
            out.collect(new Tuple2<Integer, String>(key, s));
        }
    }
}