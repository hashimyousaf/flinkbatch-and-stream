import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class TableApiStreaming {

    public static void main(String[] args){
        System.out.println("hashim");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // get a batch table environment
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.getTableEnvironment(env);

//        tableEnvironment.registerTableSource("UserActions", new UserActionSource());

        WindowedTable windowedTable = tableEnvironment
                .scan("UserActions")
                .window(Tumble.over("10.minutes").on("UserActionTime").as("userActionWindow"));

    }

    // define a table source with a processing attribute
//    public static class UserActionSource implements StreamTableSource<Row>, DefinedProctimeAttribute {
//
//        public TableSchema getTableSchema() {
//            return null;
//        }
//
//        public TypeInformation<Row> getReturnType() {
//            String[] names = new String[] {"movie" , "genre"};
//
//            TypeInformation[] types = new TypeInformation[] {Types.STRING, Types.STRING};
//            return (TypeInformation<Row>) Types.ROW(names, types);
//        }
//
//        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
//            // create stream
//            DataStream<Row> stream = ...;
//            return stream;
//        }
//
//        public String getProctimeAttribute() {
//            // field with this name will be appended as a third field
//            return "UserActionTime";
//        }
//    }
}
