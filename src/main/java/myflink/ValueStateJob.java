package myflink;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ValueStateJob {
    public static void main(String[] args) throws Exception {

        int example = 2;
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
        env.enableCheckpointing(100);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        if (params.has("p")) {
            env.setParallelism(Integer.parseInt(params.get("p")));
        } else {
            env.setParallelism(1);
        }

        switch(example){
            case 1:
                countWindowAverage(env);
                break;
            case 2:
                countWindowAverageCrash(env);
                break;
            default:
                break;
        }


    }

    static void countWindowAverage(StreamExecutionEnvironment env) throws Exception {
        System.out.println("Job: CountWindowAverage");

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();
        // the printed output will be (1,4) and (1,5)
        env.execute("Count Window Average");
    }

    static void countWindowAverageCrash(StreamExecutionEnvironment env) throws Exception {
        System.out.println("Job: CountWindowAverageCrash");
        boolean kaboom = true;

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L), Tuple2.of(8L, 3L), Tuple2.of(4L, 4L), Tuple2.of(2L, 7L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();
        // the printed output will be (1,4) and (1,5)
        env.execute("Count Window Average Crash");
    }

}

class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    private boolean kaboom = true;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // Cause node failure in middle of stream processing
        if(input.f0 == 1L && input.f1 == 2L && kaboom){
            kaboom = false;
            throw new Exception("KABOOM!");
        }

        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}