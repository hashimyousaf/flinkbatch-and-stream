import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class DemoClass {

    // This is WordCount using flapMap
    public static void main(String[] args) throws Exception {
        System.out.println("This is hashim yousaf");
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        //==============Simple FlatMap Function example========================
//        DataSource<Tuple1<String>> textLines =  env.readCsvFile("src/ml-latest-small/short-movies.csv")
//                .ignoreFirstLine()
//                .includeFields(false, false, true)
//                .parseQuotedStrings('"')
//                .ignoreInvalidLines()
//                .types(String.class );
//
//        DataSet<String> listOfGenre = textLines.flatMap(new FlatMapFunction<Tuple1<String>, String>() {
//            public void flatMap(Tuple1<String> value, Collector<String> collector) throws Exception {
//
//                for (String token :  value.toString().split("|")) {
//                    collector.collect(token);
//                }
//
//            }
//        });
//        listOfGenre.print();


        //==============Simple Map Function example========================
//        DataSource<Tuple2<Integer, Integer>> intPairs =  env.readCsvFile("src/ml-latest-small/short-ratings.csv")
//                .ignoreFirstLine()
//                .includeFields(true, true, false, false)
//                .parseQuotedStrings('"')
//                .ignoreInvalidLines()
//                .types(Integer.class, Integer.class);
//
//        DataSet<Integer> intSums = intPairs.map(new IntAdder());
//        intSums.print();


        //==============WordCount with flapMap Example========================
//        DataSet<Tuple3<Long, String, String>> lines =  env.readCsvFile("src/ml-latest-small/short-movies.csv")
//                .ignoreFirstLine()
//                .parseQuotedStrings('"')
//                .ignoreInvalidLines()
//                .types(Long.class, String.class, String.class);
//        lines.print();
//
//
//        DataSet<String> text = env.fromElements(
//                "Who's there?",
//                "I think I hear them. Stand, ho! Who's there?");
//        DataSet<Tuple2<String, Integer>> wordCounts = text
//                .flatMap(new LineSplitter())
//                .groupBy(0)
//                .sum(1);
//        wordCounts.print();
    }


    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    // MapFunction that adds two integer values
    public static class IntAdder implements MapFunction<Tuple2<Integer, Integer>, Integer> {

        public Integer map(Tuple2<Integer, Integer> in) {
            return in.f0 + in.f1;
        }
    }
}
