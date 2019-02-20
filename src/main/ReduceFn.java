import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReduceFn {

    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<WC> words = env.readCsvFile("src/ml-latest-small/demo.csv")
                .pojoType(WC.class, "word", "count");

                DataSet<WC> wordCounts = words
                        // DataSet grouping on field "word"
                        .groupBy("word")
                        // apply ReduceFunction on grouped DataSet
                        .reduce(new WordCounter());
                words.print();

    }

}

// ReduceFunction that sums Integer attributes of a POJO
class WordCounter implements ReduceFunction<WC> {
    public WC reduce(WC in1, WC in2) {
        return new WC(in1.word, in1.count + in2.count);
    }
    public static void demoFunction(){

    }

}


