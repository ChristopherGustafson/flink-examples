package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

public class MapStateJob {
    public static void main(String[] args) throws Exception {

        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        //env.setStateBackend(new Rocks)
        Configuration config = new Configuration();
//        config.setString("state.backend", "filesystem");
//        config.setString("state.backend", "rocksdb");
        config.setString("state.backend", "ndb");
        config.setString("state.backend.ndb.connectionstring", "127.0.0.1");
        config.setString("state.backend.ndb.dbname", "flinkndb");
        config.setString("state.backend.ndb.truncatetableonstart", "false");
        config.setString("execution.checkpointing.checkpoints-after-tasks-finish.enabled", "true");

        config.setString("state.savepoints.dir", "file:///tmp/flinksavepoints");
        config.setString("state.checkpoints.dir", "file:///tmp/flinkcheckpoints");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        /* CHECKPOINTING */
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setParallelism(2);


        int example = 4;
        switch (example) {
            case 1:
                basicMapOperation(env);
                break;
            case 2:
                basicMapOperationCrash(env);
                break;
            case 3:
                wordCountMapOperationCrash(env);
                break;
            case 4:
                wordCountOperationCrash(env);
            default:
                break;
        }
    }

    static void basicMapOperation(StreamExecutionEnvironment env) throws Exception {
        env.execute();
    }

    static void basicMapOperationCrash(StreamExecutionEnvironment env) throws Exception {
        env.execute();
    }

    static void wordCountMapOperationCrash(StreamExecutionEnvironment env) throws Exception {
        //open socket with nc -l 9999 before running the program
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Long>> count =
                data.flatMap(new FlatMapFunction<String, String>() {
                            @Override
                            public void flatMap(String line, Collector<String> collector) throws Exception {

                                String[] words = line.split(" ");
                                String firstWord = words[0];
                                if (firstWord.equals("flinkNDB")) {
                                    throw new FlinkRuntimeException("KABOOM!");
                                }
                                collector.collect(firstWord);
                            }
                        })
                        //make a keyed stream based on the first keyword
                        .keyBy(word -> word)

                        //use manual state to count the words
                        .flatMap(new RichFlatMapFunction<String, Tuple2<String, Long>>() {

                            MapState<String, Long> countMapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                countMapState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<String, Long>("countMapState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));
                            }

                            @Override
                            public void flatMap(String input,
                                                Collector<Tuple2<String, Long>> collector) throws Exception {
                                ;
                                Long size = countMapState.get(input);
                                if(size != null){
                                    size++;
                                    countMapState.put(input, size);
                                }
                                else{
                                    size = 1L;
                                    countMapState.put(input, size);
                                }
                                collector.collect(new Tuple2<>(input, size));
                            }
                        });
        count.print();
        env.execute("Map WordCount with crash");
    }


    static void wordCountOperationCrash(StreamExecutionEnvironment env) throws Exception {
        //open socket with nc -l 9999 before running the program
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Tuple3<Long, Long, Long>>> count =
                data.flatMap(new FlatMapFunction<String, String>() {
                            @Override
                            public void flatMap(String line, Collector<String> collector) throws Exception {

                                String[] words = line.split(" ");
                                String firstWord = words[0];
                                if (firstWord.equals("flinkNDB")) {
                                    throw new FlinkRuntimeException("KABOOM!");
                                }
                                collector.collect(firstWord);
                            }
                        })
                        //make a keyed stream based on the first keyword
                        .keyBy(word -> word)

                        //use manual state to count the words
                        .flatMap(new RichFlatMapFunction<String, Tuple2<String, Tuple3<Long, Long, Long>>>() {
                            ValueState<Long> countValueState;
                            ListState<String> countListState;
                            MapState<String, Long> countMapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Long>("countValueState", BasicTypeInfo.LONG_TYPE_INFO));
                                countListState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<String>("countListState", BasicTypeInfo.STRING_TYPE_INFO));
                                countMapState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<String, Long>("countMapState", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO));
                            }

                            @Override
                            public void flatMap(String input,
                                                Collector<Tuple2<String, Tuple3<Long, Long, Long>>> collector) throws Exception {
                                ;

                                long sizeV = 1L;
                                if(countValueState.value() != null){
                                    sizeV = countValueState.value()+1L;
                                }
                                countValueState.update(sizeV);

                                countListState.add(input);
                                long sizeL = 0L;
                                for(String t : countListState.get()){
                                    sizeL++;
                                }

                                Long sizeM = countMapState.get(input);
                                if(sizeM != null){
                                    sizeM++;
                                    countMapState.put(input, sizeM);
                                }
                                else{
                                    sizeM = 1L;
                                    countMapState.put(input, sizeM);
                                }
                                collector.collect(new Tuple2<>(input, new Tuple3<>(sizeV, sizeL, sizeM)));
                            }
                        });
        count.print();
        env.execute("All State types WordCount with crash");
    }

}
