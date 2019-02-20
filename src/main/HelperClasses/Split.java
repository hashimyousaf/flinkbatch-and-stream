package HelperClasses;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

// The generic type "Tuple2<String, Integer>" determines the schema of the returned table as (String, Integer).
public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";

    public Split(String separator) {
        this.separator = separator;
    }

    public void eval(String str) {
        // collecting more than one value in particular case
//        for (String s : str.split(separator)) {
//            // use collect(...) to emit a row
//            collect(new Tuple2<String, Integer>(s, s.length()));
//        }
        // getting one value
        String[] array = str.split("\\|");
        collect(new Tuple2<String, Integer>(str, array.length));
    }
}

