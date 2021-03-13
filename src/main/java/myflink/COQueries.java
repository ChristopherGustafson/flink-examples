package myflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

public class COQueries {

    public static void main(String[] args) throws Exception {


        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */

        int example;
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        //env.setStateBackend(new Rocks)
        Configuration config = new Configuration();
        //config.setString("state.backend", "filesystem");


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


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);


        example = 4;//106;

        switch (example) {
            case 1:
                Query1(env);
            case 2:
                Query2(env);
            case 4:
                Query4(env);
            default:
                break;
        }
    }

    private static void Query1(StreamExecutionEnvironment env) throws Exception {
        File file = new File("src/main/resources/data.txt");
        String absolutePath = file.getAbsolutePath();

        //env.readFile(FileInputFormat.  absolutePath, )

        //open socket with nc -l 9999 before running the program
        DataStream<String> data = env.readTextFile(absolutePath);
        //env.socketTextStream("localhost", 9999);

        DataStream<Tuple3<Integer, String, Integer>> count =
                data
                        .map(new MapFunction<String, Tuple3<Integer, String, Integer>>() {
                            @Override
                            public Tuple3<Integer, String, Integer> map(String s) throws Exception {
                                String[] cols = s.split(",");
                                return new Tuple3<>(Integer.parseInt(cols[1]), cols[16], 1); //county, arithmatic mean, count
                            }
                        })
                        .filter(row -> row.f0 == 31)
                        .keyBy(0) //similar to group in batch processing
                        .sum(2);

        count.print();

        env.execute("Query filtered sum");
    }

    //Which state has more emission  - Max
    //Based on all the collected data average
    private static void Query2(StreamExecutionEnvironment env) throws Exception {

        File file = new File("src/main/resources/data.txt");
        String absolutePath = file.getAbsolutePath();

        DataStream<String> data = env.readTextFile(absolutePath);

        DataStream<Tuple2<Integer, Double>> count =
                data
                        .map(new MapFunction<String, Tuple3<Integer, Double, Integer>>() {
                            @Override
                            public Tuple3<Integer, Double, Integer> map(String s) throws Exception {
                                String[] cols = s.split(",");
                                return new Tuple3<>(Integer.parseInt(cols[0]), Double.parseDouble(cols[16]), 1); //state code, arithmatic mean, count
                            }
                        })
                        .keyBy(0) //similar to group in batch processing
                        .reduce(new ReduceFunction<Tuple3<Integer, Double, Integer>>() {
                            @Override
                            public Tuple3<Integer, Double, Integer> reduce(Tuple3<Integer, Double, Integer> t2,
                                                                           Tuple3<Integer, Double, Integer> t1) throws Exception {
                                return new Tuple3<>(t1.f0, t1.f1 + t2.f1, t1.f2 + t2.f2);
                            }
                        })
                        .map(new MapFunction<Tuple3<Integer, Double, Integer>, Tuple2<Integer, Double>>() {
                            @Override
                            public Tuple2<Integer, Double> map(Tuple3<Integer, Double, Integer> t3) throws Exception {
                                return new Tuple2<Integer, Double>(t3.f0, t3.f1 / t3.f2);
                            }
                        })

                        .keyBy(0)
                        .max(1);


        count.print();

        env.execute("Query filtered sum");
    }

    //Which state has lowest emission with a window size of one year (

    private static void Query4(StreamExecutionEnvironment env) throws Exception {

        File file = new File("src/main/resources/data.txt");
        String absolutePath = file.getAbsolutePath();

        DataStream<String> data = env.readTextFile(absolutePath);

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        DataStream<Tuple3<Integer, Double, Integer>> count =
                data
                        .map(new MapFunction<String, Tuple4<Integer, Double, Date, Integer>>() {
                            @Override
                            public Tuple4<Integer, Double, Date, Integer> map(String s) throws Exception {
                                String[] cols = s.split(",");
                                return new Tuple4<>(Integer.parseInt(cols[0]),
                                        Double.parseDouble(cols[16]),
                                        dateFormat.parse(cols[28]),
                                        1);
                                //0 state code, 16 arithmatic mean, 28 date, count
                            }
                        })
                        .keyBy(0) //Key be state code, then we will do windowsing
                        .window(TumblingProcessingTimeWindows.of(Time.days(365)))
                        .reduce(new ReduceFunction<Tuple4<Integer, Double, Date, Integer>>() {
                            @Override
                            public Tuple4<Integer, Double, Date, Integer> reduce(Tuple4<Integer, Double, Date, Integer> t2,
                                                                                 Tuple4<Integer, Double, Date, Integer> t1) throws Exception {
                                return new Tuple4<>(t1.f0, t1.f1 + t2.f1, t1.f2, t1.f3 + t2.f3);
                            }
                        })
                        .map(new MapFunction<Tuple4<Integer, Double, Date, Integer>, Tuple3<Integer, Double, Integer>>() {
                            @Override
                            public Tuple3<Integer, Double, Integer> map(Tuple4<Integer, Double, Date, Integer> t3) throws Exception {
                                return new Tuple3<>(t3.f0, t3.f1 / t3.f3, t3.f2.getYear());
                            }
                        })

                        .keyBy(0)
                        .min(1);


        count.print();

        env.execute("Query filtered sum");
    }

}
