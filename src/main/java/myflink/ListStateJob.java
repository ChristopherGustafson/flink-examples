package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Arrays;
import java.util.List;

public class ListStateJob {
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
        config.setString("state.backend.ndb.lazyrecovery", "true");
        config.setString("execution.checkpointing.checkpoints-after-tasks-finish.enabled", "true");

        config.setString("state.savepoints.dir", "file:///tmp/flinksavepoints");
        config.setString("state.checkpoints.dir", "file:///tmp/flinkcheckpoints");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        /* CHECKPOINTING */
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setParallelism(2);

        int example = 3;
        switch (example) {
            case 1:
                basicListOperation(env);
                break;
            case 2:
                basicListOperationCrash(env);
            case 3:
                wordCountListOperationCrash(env);
                break;
            case 4:
                wordCountValueOperationClear(env);
                break;
            default:
                break;
        }
    }

    static void basicListOperation(StreamExecutionEnvironment env) throws Exception {
        // Set stream source as
        env.fromSequence(0, 10000000)
                .keyBy(l -> l%10)
                .flatMap(new RichFlatMapFunction<Long, Long>(){
                    ListState<Long> divisibleBy5;
                    @Override
                    public void flatMap(Long input, Collector<Long> out) throws Exception {
                        if(input % 5 == 0){
                            divisibleBy5.add(input);

                            for(Long l : divisibleBy5.get()){
                                System.out.print(l+", ");
                            }
                            System.out.print("\n");
                        }
                        out.collect(input);
                    }
                    @Override
                    public void open(Configuration config) {
                        ListStateDescriptor<Long> descriptor =
                                new ListStateDescriptor<>(
                                        "divisible", // the state name
                                        TypeInformation.of(new TypeHint<Long>() {}));
                        divisibleBy5 = getRuntimeContext().getListState(descriptor);
                    }
        });
        env.execute();
    }

    static void basicListOperationCrash(StreamExecutionEnvironment env) throws Exception {
        // Set stream source as
        env.fromSequence(0, 1000)
                .keyBy(l -> l%10)
                .flatMap(new FailingMapper());
        env.execute();
    }

    private static class FailingMapper extends RichFlatMapFunction<Long, Long>{
        ListState<Long> divisibleBy5;
        private static volatile int failures = 3;
        @Override
        public void flatMap(Long input, Collector<Long> out) throws Exception {
            if(failures > 0 && input == 445L){
                failures--;
                throw new RuntimeException("KABOOM!");
            }

            if(input % 5 == 0){
                divisibleBy5.add(input);
            }
            out.collect(input);
        }
        @Override
        public void open(Configuration config) {
            ListStateDescriptor<Long> descriptor =
                    new ListStateDescriptor<>(
                            "divisible", // the state name
                            TypeInformation.of(new TypeHint<Long>() {}));
            divisibleBy5 = getRuntimeContext().getListState(descriptor);
        }
    }

    static void wordCountListOperationCrash(StreamExecutionEnvironment env) throws Exception {
        //open socket with nc -l 9999 before running the program
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Long>> count =
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {

                String[] words = line.split(" ");
                String firstWord = words[0];
                if (firstWord.equals("flinkNDB")) {
                    throw new FlinkRuntimeException("Ahah");
                }

                collector.collect(firstWord);
            }
        })
        //make a keyed stream based on the first keyword
        .keyBy(word -> word)

        //use manual state to count the words
        .flatMap(new RichFlatMapFunction<String, Tuple2<String, Long>>() {

            ListState<String> countListState;

            @Override
            public void open(Configuration parameters) throws Exception {
                countListState = getRuntimeContext().getListState(
                        new ListStateDescriptor<String>("countListState", BasicTypeInfo.STRING_TYPE_INFO));
            }

            @Override
            public void flatMap(String input,
                                Collector<Tuple2<String, Long>> collector) throws Exception {
;
                countListState.add(input);
                long size = 0;
                for(String t : countListState.get()){
                    size++;
                }
                collector.collect(new Tuple2<>(input, size));
            }
        });
        count.print();
        env.execute("List count example execution");

    }

    static void wordCountValueOperationClear(StreamExecutionEnvironment env) throws Exception {
        //open socket with nc -l -k 9999 before running the program
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Long>> count =
                data.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {

                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, String>> collector) throws Exception {

                                String[] words = line.split(" ");
                                String firstWord = words[0];
                                String secondWord = words.length > 1 ? words[1] : "";
                                collector.collect(new Tuple2(firstWord, secondWord));
                            }
                        })
                        //make a keyed stream based on the first keyword
                        .keyBy(tuple -> tuple.f0)

                        //use manual state to count the words
                        .flatMap(new RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Long>>() {

                            ListState<String> countListState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                countListState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<String>("countListState", BasicTypeInfo.STRING_TYPE_INFO));
                            }

                            @Override
                            public void flatMap(Tuple2<String, String> input,
                                                Collector<Tuple2<String, Long>> collector) throws Exception {

                                long size = 0;
                                if(input.f1.equals("clear")){
                                    System.out.println("Clearing list state for key " + input.f0);
                                    countListState.clear();
                                }
                                else{
                                    countListState.add(input.f0);
                                    for(String t : countListState.get()){
                                        size++;
                                    }
                                }

                                collector.collect(new Tuple2<>(input.f0, size));
                            }
                        });
        count.print();
        env.execute("List count example execution");
    }


}
