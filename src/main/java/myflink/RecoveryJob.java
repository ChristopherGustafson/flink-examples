package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Arrays;
import java.util.List;

public class RecoveryJob {

    public static void main(String[] args) throws Exception {

        int example;
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

        example = 3;

        /* CHECKPOINTING */
        env.enableCheckpointing(10);

        if (params.has("p")) {
            env.setParallelism(Integer.parseInt(params.get("p")));
        } else {
            env.setParallelism(4);
        }

        switch(example){
            case 1:
                wordCountCrash(env);
                break;
            case 2:
                testRestartMultipleTimes(env);
                break;
            case 3:
                countWindowAverage(env);
                break;
            case 4:
                wordCountAllStateTypesCrash(env);
            default:
                break;
        }
    }

    static void wordCountAllStateTypesCrash(StreamExecutionEnvironment env) throws Exception {
        //open socket with nc -l -k 9999 before running the program
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


    private static void wordCountCrash(StreamExecutionEnvironment env) throws Exception {

        DataStreamSource<String> source = env.fromElements(
                "Newcastle",
                "Man United",
                "Man City",
                "Arsenal",
                "Liverpool",
                "Newcastle",
                "Newcastle",
                "Arsenal",
                "Man City"
        );

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                source.flatMap(( String value, Collector<Tuple2<String, Integer>> out ) -> {
                            // normalize and split the line into words
                            String[] tokens = value.toLowerCase().split( "\\W+" );

                            // emit the pairs
                            for( String token : tokens ){
                                if( token.length() > 0 ){
                                    out.collect( new Tuple2<>( token, 1 ) );
                                }
                            }
                        } )
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0)
                        .sum( 1 );

        counts.writeAsText("output");

        env.execute("List count example execution");
    }

    private static void countWindowAverage(StreamExecutionEnvironment env) throws Exception{
        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L)
                        , Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L), Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();
        env.execute("Count Window Average");

    }

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        /**
         * The ValueState handle. The first field is the count, the second field a running sum.
         */
        private transient ValueState<Tuple2<Long, Long>> sum;
        private transient int failures = 1;
        private transient int operationsUntilFailure = 3;

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

            if(failures > 0 && operationsUntilFailure < 1){
                failures--;
                throw new RuntimeException("KABOOM!");
            }
            operationsUntilFailure--;

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



    private static void testRestartMultipleTimes(StreamExecutionEnvironment env) throws Exception {
        try {
            System.out.println("Test Restart Multiple Times");
            List<Long> resultCollection =
                    env.fromSequence(1, 1000)
                            .keyBy(value -> value)
                            .map(new FailingMapper1<>())
                            .executeAndCollect(1000);

            long sum = 0;
            for (long l : resultCollection) {
                sum += l;
            }
            System.out.println(sum);
//            assert 55 == sum;
        } finally {
            FailingMapper1.failuresBeforeSuccess = 1;
        }
    }


    private static class FailingMapper1<T> extends RichMapFunction<T, T> {

        private static volatile int operationsBeforeFailure = 500;
        private static volatile int failuresBeforeSuccess = 1;

        @Override
        public T map(T value) throws Exception {
            System.out.println("Processing value " + value);
            Thread.sleep(100);
            if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 0 && operationsBeforeFailure < 0) {
                failuresBeforeSuccess--;
                throw new Exception("Test Failure");
            }
            operationsBeforeFailure--;

            return value;
        }
    }

    private static class FailingMapper2<T> extends RichMapFunction<T, T> {

        private static volatile int failuresBeforeSuccess = 1;

        @Override
        public T map(T value) throws Exception {
            if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
                failuresBeforeSuccess--;
                throw new Exception("Test Failure");
            }

            return value;
        }
    }

    private static class FailingMapper3<T> extends RichMapFunction<T, T> {

        private static volatile int failuresBeforeSuccess = 3;

        @Override
        public T map(T value) throws Exception {

            if (failuresBeforeSuccess > 0 && getRuntimeContext().getIndexOfThisSubtask() == 1) {
                failuresBeforeSuccess--;
                throw new Exception("Test Failure");
            }

            return value;
        }
    }
}


