
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapPartition {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);  // by default it's dependant on number of cores of your computer

        DataSet<String> textLines =   env.readTextFile("src/ml-latest-small/demo.txt");
        DataSet<Long> counts = textLines.mapPartition(new PartitionCounter());
        counts.print();

    }

    public static class PartitionCounter implements MapPartitionFunction<String, Long> {

        public void mapPartition(Iterable<String> values, Collector<Long> out) {
            long c = 0;
            for (String s : values) {
                c++;
            }
            out.collect(c);
        }
    }
}
