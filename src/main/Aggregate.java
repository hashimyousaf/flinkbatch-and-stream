package main;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class Aggregate {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple1<Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .filter(new FilterFunction<String>() {
                    public boolean filter(String s) throws Exception {
                        try {
                            Integer.parseInt(s);
                            return true;
                        } catch (Exception e) {
                        }
                        return false;
                    }
                })
                .flatMap(new Splitter())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(0);

        dataStream.print();


        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple1<Integer>> {
        public void flatMap(String sentence, Collector<Tuple1<Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                Integer intValue = Integer.parseInt(word);
                out.collect(new Tuple1< Integer>(intValue));
            }
        }
    }
        /*
        * flatMap(new FlatMapFunction<Tuple1<String>, String>() {
//            public void flatMap(Tuple1<String> value, Collector<String> collector) throws Exception {
//
//                for (String token :  value.toString().split("|")) {
//                    collector.collect(token);
//                }
//
//            }
*/
//                .map(new MapFunction<String, Integer>() {
//                    public Integer map(String s) throws Exception{
//                        return   Integer.parseInt(s);
//                    }
//                }).print();
        //text
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                        for (String word : line.split(" ")) {
//                            collector.collect(new Tuple2<String, Integer>(word, 1));
//                        }
//                    }
//                }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).setParallelism(5);


//        DataSource<Tuple3<Integer ,String, Double>> input =  env.readCsvFile("src/ml-latest-small/aggregate.csv")
//                .ignoreFirstLine()
//                .includeFields(true, true, true, false)
//                .types(Integer.class, String.class, Double.class); // Pay(0), Employee_name(1), fine(2)
//
//        DataSet<Tuple3<Integer, String, Double>> output = input
//                .groupBy(1)        // group DataSet on second field
//                .aggregate(Aggregations.SUM, 0) // compute sum of the first field
//                .and(Aggregations.MIN, 2);
//
//        output.print();


}
