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

public class ValueStateJob {
    public static void main(String[] args) throws Exception {

        int example = 3;
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

        //state-Backend
        if (params.has("sb")) {
            config.setString("state.backend", params.get("sb"));
        }

        //example
        if (params.has("e")) {
            example = Integer.parseInt(params.get("e"));
        }


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        //config.setString("web.timeout", "100000");
        // set up the streaming execution environment
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        /* CHECKPOINTING */
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        if (params.has("p")) {
            env.setParallelism(Integer.parseInt(params.get("p")));
        } else {
            env.setParallelism(1);
        }

        switch(example){
            case 1:
                basicValueOperationCrash(env);
            case 2:
                wordCountValueOperationCrash(env);
                break;
            case 3:
                wordCountValueOperationClear(env);
                break;
            default:
                break;
        }


    }

    static void basicValueOperationCrash(StreamExecutionEnvironment env) throws Exception {
        // Set stream source as
        env.fromSequence(0, 100000)
                .keyBy(l -> l%10)
                .flatMap(new FailingMapper());
        env.execute();
    }

    private static class FailingMapper extends RichFlatMapFunction<Long, Long>{
        ValueState<Long> divisibleBy5;
        private static volatile int failures = 1;
        @Override
        public void flatMap(Long input, Collector<Long> out) throws Exception {
            if(failures > 0 && input == 9995L){
                failures--;
                throw new RuntimeException("KABOOM!");
            }

            if(input % 5 == 0){
                long count = divisibleBy5.value();
                divisibleBy5.update(count+1L);
            }
            out.collect(input);
        }
        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Long> descriptor =
                    new ValueStateDescriptor<>(
                            "divisible", // the state name
                            TypeInformation.of(new TypeHint<Long>() {}),
                            0L);
            divisibleBy5 = getRuntimeContext().getState(descriptor);
        }
    }

    static void wordCountValueOperationCrash(StreamExecutionEnvironment env) throws Exception {
        //open socket with nc -l -k 9999 before running the program
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

                            ValueState<Long> countValueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Long>("countValueState", BasicTypeInfo.LONG_TYPE_INFO));
                            }

                            @Override
                            public void flatMap(String input,
                                                Collector<Tuple2<String, Long>> collector) throws Exception {

                                long size = 0L;
                                if(countValueState.value() != null){
                                    size = countValueState.value()+1L;
                                }
                                countValueState.update(size);
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

                            ValueState<Long> countValueState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Long>("countValueState", BasicTypeInfo.LONG_TYPE_INFO));
                            }

                            @Override
                            public void flatMap(Tuple2<String, String> input,
                                                Collector<Tuple2<String, Long>> collector) throws Exception {
                                long size = 0L;
                                if(input.f1.equals("clear")){
                                    System.out.println("Clearing values state for key " + input.f0);
                                    countValueState.clear();
                                }
                                else{
                                    if(countValueState.value() != null){
                                        size = countValueState.value()+1L;
                                    }
                                    countValueState.update(size);
                                }
                                collector.collect(new Tuple2<>(input.f0, size));
                            }
                        });
        count.print();
        env.execute("List count example execution");
    }

}
