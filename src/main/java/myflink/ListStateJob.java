package myflink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
        config.setString("execution.checkpointing.checkpoints-after-tasks-finish.enabled", "true");

        config.setString("state.savepoints.dir", "file:///tmp/flinksavepoints");
        config.setString("state.checkpoints.dir", "file:///tmp/flinkcheckpoints");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        /* CHECKPOINTING */
        env.enableCheckpointing(10);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setParallelism(2);

        int example = 2;
        switch (example) {
            case 1:
                basicListOperation(env);
                break;
            case 2:
                basicListOperationCrash(env);
            default:
                break;
        }
    }

    static void basicListOperation(StreamExecutionEnvironment env) throws Exception {
        // Set stream source as
        env.fromSequence(0, 1000)
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
        private static volatile int failures = 1;
        @Override
        public void flatMap(Long input, Collector<Long> out) throws Exception {
            if(failures > 0 && input == 445L){
                failures--;
                throw new RuntimeException("KABOOM!");
            }

            if(input % 5 == 0){
                divisibleBy5.add(input);
                System.out.println("Current state of list: ");
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
    }
}
