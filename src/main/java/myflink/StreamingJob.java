/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink;

import akka.dispatch.Foreach;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.File;
import java.util.*;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

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
        //config.setString("state.backend","filesystem");
        //config.setString("state.backend", "rocksdb");
        config.setString("state.backend", "ndb");
        config.setString("state.backend.ndb.connectionstring", "localhost");
        config.setString("state.backend.ndb.dbname", "flinkndb");
        config.setString("state.backend.ndb.truncatetableonstart", "true");


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

        //config.setString("web.timeout", "100000");
        // set up the streaming execution environment
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        example = 106;
        env.enableCheckpointing(25000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        if (params.has("p")) {
            env.setParallelism(Integer.parseInt(params.get("p")));
        } else {
            env.setParallelism(1);
        }

        //env.getConfig().setGlobalJobParameters(config); not working

        //Testing - Done
        //valueState  - 1,3, 4, 101-Clear, 11,12
        //List State - 102
        //Reducing State - 103
        //aggregate State - 104
        //Map State - 105

        //Failed
        //priorityQueue -- 2

//        env.addSource(new SourceFunction<List<Person>>() {
//            @Override
//            public void run(SourceContext<List<Person>> sourceContext) throws Exception {
//
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        });

        switch (example) {
            case 1:
                StatefulCoFlatmap(env);
                break;
            case 2:
                simpleStateFulStreamExample(env);
                break;
            case 3:
                WordCountExample(env); //need to start the external util
                break;
            case 4:
                WordCountExampleFromFile(env); //Does use value state automatically
                break;
            case 5:
                UdemyCourseAssignment(env);
            case 6:
                WindowExample(env);
            case 7:
                SessionWindowExample(env);
            case 8:
                GlobalWindowExample(env);
            case 9:
                CountTriggerWindowExample(env);
            case 10:
                UdemyCourseAssignment2(env);
            case 11:
                KeyByFun(env);
                break;
            case 12:
                ReduceExample(env);
                break;

            //state examples starting with 1**
            case 101:
                SumByStatelessOperatorsUsingValueState(env);
                break;
            case 102:
                SumByStatelessOperatorsUsingListState(env);
                break;
            case 103:
                SumByStatelessOperatorsUsingReducingState(env);
                break;
            case 104:
                SumByStatelessOperatorsUsingAggregateState(env);
                break;
            case 105:
                SumByStatelessOperatorsUsingMapState(env);
            case 106:
                WordCountUsingMapStateUntilThree(env);
            default:
                break;
        }
    }

    private static void WordCountExample(StreamExecutionEnvironment env) throws Exception {

        //open socket with nc -l 9999 before running the program
        DataStream<String> data = env.socketTextStream("localhost", 9999).uid("SocketTextStream-id");

        DataStream<Tuple2<String, Integer>> count =
                data.filter(s -> s.startsWith("n"))
                        .map(new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String s) throws Exception {
                                return new Tuple2<>(s, 1);
                            }
                        }).uid("Filter-Map-id").keyBy(0) //similar to group in batch processing
                        .sum(1).uid("KeyBy-sum-id");

        count.print();

        env.execute("Word count example execution");
    }

    private static void WordCountExampleFromFile(StreamExecutionEnvironment env) throws Exception {
        File file = new File("src/main/resources/wc.txt");
        String absolutePath = file.getAbsolutePath();

        //env.readFile(FileInputFormat.  absolutePath, )

        //open socket with nc -l 9999 before running the program
        DataStream<String> data = env.readTextFile(absolutePath);
        //env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> count =
                data
                        .map(new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String s) throws Exception {
                                return new Tuple2<>(s, 1);
                            }
                        })
                        .keyBy(0) //similar to group in batch processing
                        .sum(1);

        count.print();

        env.execute("Word count example execution");
    }

    private static void WindowExample(StreamExecutionEnvironment env) throws Exception {

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //open socket with nc -l 9999 before running the program
        // it will send alphabet, count {(a,2), (b,5)}
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> mapped =
                data.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
                    }
                });

        DataStream<Tuple2<String, Integer>> count =
                mapped.keyBy(0)
                        //.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                        .reduce((ReduceFunction<Tuple2<String, Integer>>) (current, pre) -> new Tuple2<>(current.f0, pre.f1 + current.f1));
        count.print();

        env.execute("Word count example execution");
    }

    private static void SessionWindowExample(StreamExecutionEnvironment env) throws Exception {

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //open socket with nc -l 9999 before running the program
        // it will send alphabet, count {(a,2), (b,5)}
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> mapped =
                data.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
                    }
                });

        DataStream<Tuple2<String, Integer>> count =
                mapped
                        .keyBy(0)
                        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(15)))
                        .reduce((ReduceFunction<Tuple2<String, Integer>>) (current, pre) -> new Tuple2<>(current.f0, pre.f1 + current.f1));
        count.print();

        env.execute("Word count example execution");
    }

    private static void GlobalWindowExample(StreamExecutionEnvironment env) throws Exception {

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //open socket with nc -l 9999 before running the program
        // it will send alphabet, count {(a,2), (b,5)}
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> mapped =
                data.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
                    }
                });

        DataStream<Tuple2<String, Integer>> count =
                mapped
                        .keyBy(0)
                        .window(GlobalWindows.create())
                        .trigger(CountTrigger.of(2))
                        .reduce((ReduceFunction<Tuple2<String, Integer>>) (current, pre) -> new Tuple2<>(current.f0, pre.f1 + current.f1));
        count.print();

        env.execute("example execution");
    }

    private static void ReduceExample(StreamExecutionEnvironment env) throws Exception {
        File file = new File("src/main/resources/productProfit.txt");
        String absolutePath = file.getAbsolutePath();

        DataStream<String> data = env.readTextFile(absolutePath);

        //Map into tuple for each column
        DataStream<Tuple4<String, String, Integer, Integer>> mapped =
                data.map((MapFunction<String, Tuple4<String, String, Integer, Integer>>) s -> {
                    String[] split = s.split(",");
                    return new Tuple4<>(split[0], split[1], Integer.parseInt(split[2].trim()), 1);
                })
                        .returns(new TypeHint<Tuple4<String, String, Integer, Integer>>() {
                            @Override
                            public TypeInformation<Tuple4<String, String, Integer, Integer>> getTypeInfo() {
                                return super.getTypeInfo();
                            }
                        });

        //Group by product ID
        DataStream<Tuple4<String, String, Integer, Integer>> reduced =
                mapped.keyBy(1).reduce((ReduceFunction<Tuple4<String, String, Integer, Integer>>) (current, pre)
                        -> new Tuple4<>(current.f0, current.f1, current.f2 + pre.f2, current.f3 + pre.f3));

        //calculate the average for each product <productID, average>
        DataStream<Tuple2<String, Double>> average =
                reduced.map(new MapFunction<Tuple4<String, String, Integer, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple4<String, String, Integer, Integer> input) throws Exception {
                        return new Tuple2<>(input.f1, input.f2 * 1.0 / input.f3);
                    }
                });

        average.print();


        env.execute("Reduce Example");

    }

    private static void StatefulCoFlatmap(StreamExecutionEnvironment env) throws Exception {
        //We will retain the check pointing and see how the data look like at the above given path.
//		CheckpointConfig chkpointcfg= env.getCheckpointConfig();
//		chkpointcfg.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStream<Person> data = env.fromElements(
                new Person(1, "Haseeb", 30, 0),
                new Person(2, "sruthi", 20, 1),
                new Person(3, "Paris", 40, 0)
        );

        DataStream<Employee> empData = env.fromElements(
                new Employee(1, "Distributed Systems", 250000),
                new Employee(2, "Distributed Systems", 250000),
                new Employee(3, "RISE", 250000)
        );


        //RocksDBStateBackendFactory factory = new RocksDBStateBackendFactory();
        //DataStream<Person> oldPeople =  data.keyBy(x -> x.gender) .filter(person -> person.age>25);

        //oldPeople.print();
        //data.keyBy(x->x.personId).print();


        DataStream<Tuple2<Person, Employee>> salaryGender = data.keyBy(x -> x.personId)
                .connect(empData.keyBy(x -> x.personId))
                .flatMap(new RichCoFlatMapFunction<Person, Employee, Tuple2<Person, Employee>>() {

                    private ValueState<Person> personValueState;
                    private ValueState<Employee> employeeValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        personValueState = getRuntimeContext()
                                .getState(
                                        new ValueStateDescriptor<Person>("personValueSatete", Person.class)
                                );
                        employeeValueState = getRuntimeContext()
                                .getState(
                                        new ValueStateDescriptor<Employee>("empValueState", Employee.class)
                                );
                    }

                    @Override
                    public void flatMap1(Person value, Collector<Tuple2<Person, Employee>> out) throws Exception {
                        if (employeeValueState.value() != null) {
                            out.collect(new Tuple2<Person, Employee>(value, employeeValueState.value()));
                        } else {
                            personValueState.update(value);
                        }

                    }

                    @Override
                    public void flatMap2(Employee value, Collector<Tuple2<Person, Employee>> out) throws Exception {
                        if (personValueState.value() != null) {
                            out.collect(new Tuple2<Person, Employee>(personValueState.value(), value));
                        } else {
                            employeeValueState.update(value);
                        }
                    }
                });

        salaryGender.print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    private static void groupByExample(StreamExecutionEnvironment env) {

        File file = new File("src/main/resources/wc1.txt");
        String absolutePath = file.getAbsolutePath();


        DataStream<String> data = env.readTextFile(absolutePath);

        DataStream<Tuple2<Integer, Integer>> mapped =
                data.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
                    }
                });

        mapped.keyBy(new KeySelector<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> getKey(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return integerIntegerTuple2;
            }
        });

        //count.print();

        //env.execute("Word count example execution");
    }

    public static class Person {
        public Integer personId;
        public String name;
        public Integer age;
        public Integer gender;

        public Person() {
        }

        ;

        public Person(Integer personId, String name, Integer age, Integer gender) {
            this.name = name;
            this.age = age;
            this.personId = personId;
            this.gender = gender;
        }

        ;

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }

        ;
    }

    public static class Employee {
        public String departmentName;
        public Integer personId;
        public Integer salary;

        public Employee() {
        }

        ;

        public Employee(Integer personId, String departmentName, Integer salary) {
            this.departmentName = departmentName;
            this.personId = personId;
            this.salary = salary;
        }

        ;

        public String toString() {
            return this.departmentName.toString() + ": Salary " + this.salary.toString();
        }

        ;
    }


    private static void UdemyCourseAssignment(StreamExecutionEnvironment env) throws Exception {
		/*
		* Data is of the following schema
		# cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count

		Using Datastream/Dataset transformations find the following for each ongoing trip.

		1.) Popular destination.  | Where more number of people reach.

		2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.

		3.) Average number of trips for each driver.  | average =  total no. of passengers drivers has picked / total no. of trips he made

		Questions for this assignment
		What all transformation operations you will use?
		*
		*  */

        File file = new File("src/main/resources/cab-flink.txt");
        String absolutePath = file.getAbsolutePath();

        DataStream<String> data = env.readTextFile(absolutePath);

        DataStream<CabRide> mapped =
                data.map((MapFunction<String, CabRide>) s -> CabRide.fromString(s));

        //1.) Popular destination.  | Where more number of people reach.
        SingleOutputStreamOperator<Tuple2<CabRide, Integer>> result1 =
                mapped.filter(ride -> ride.DropLocation != null)
                        .map(new MapFunction<CabRide, Tuple2<CabRide, Integer>>() {
                            @Override
                            public Tuple2<CabRide, Integer> map(CabRide cabRide) throws Exception {
                                return new Tuple2<CabRide, Integer>(cabRide, 1);
                            }
                        })

                        .keyBy(new KeySelector<Tuple2<CabRide, Integer>, String>() {
                            @Override
                            public String getKey(Tuple2<CabRide, Integer> cabRideIntegerTuple2) throws Exception {
                                return cabRideIntegerTuple2.f0.DropLocation;
                            }
                        })
                        .reduce(new ReduceFunction<Tuple2<CabRide, Integer>>() {
                            @Override
                            public Tuple2<CabRide, Integer> reduce(Tuple2<CabRide, Integer> current, Tuple2<CabRide, Integer> pre) throws Exception {
                                return new Tuple2<CabRide, Integer>(current.f0, current.f1 + pre.f1);
                            }
                        })
                        .keyBy(0)
                        .max(1);

        result1.print();
        //map, filter, reduce, groupby, sum, maxby

        //2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
//		mapped.filter(ride -> ride.PickLocation != null)
//				.keyBy("PickLocation")
//				.map((MapFunction<CabRide, Tuple3<CabRide,Integer, Integer>>) ride-> new Tuple3<>(ride,1, ride.PassengerCount))
//				.keyBy("PickLocation")
//				.reduce((ReduceFunction<Tuple3<CabRide, Integer, Integer>>) (current, pre)
//						-> new Tuple3<>(current.f0, current.f1+pre.f1, current.f2+pre.f2));
//				//.keyBy("PickLocation")
//				.//fold(Tup)
//
//		mapped.filter(ride -> ride.DriverName != null)
//				.keyBy("DriverName")
//				.map((MapFunction<CabRide, Tuple3<CabRide,Integer, Integer>>) ride-> new Tuple3<>(ride,1, ride.PassengerCount))
//				.keyBy("DriverName")
//				.sum(2)
//				.keyBy("DriverName")
//				.fold(new Tuple2<String, Double>("", 0), new FoldFunction<Tuple3<CabRide, Integer, Integer>, Tuple2<String, Double>>() {
//					@Override
//					public Tuple2<String, Double> fold(Tuple2<String, Double> defalutVal, Tuple3<CabRide, Integer, Integer> current) throws Exception {
//						return new Tuple2<>(current.f0.DriverName, current.f2*1.0/current.f1 );
//					}
//				});

        env.execute("Udemy example");
    }

    private static void UdemyCourseAssignment2(StreamExecutionEnvironment env) throws Exception {

        /*
		* Data is of the following schema

		* ## user_id,network_name,user_IP,user_country,website, Time spent before next click


		 For every 10 second find out for US country

        a.) total number of clicks on every website in separate file

        b.) the website with maximum number of clicks in separate file.

        c.) the website with minimum number of clicks in separate file.

        c.) Calculate number of distinct users on every website in separate file.

        d.) Calculate the average time spent on website by users.

		Questions for this assignment
    		Is there a need to save a state for this use case?


		*
		*  */

        DataStream<String> data = ReadTextFile(env, "src/main/resources/udemyAss2.txt");


        DataStream<WebTraffic> mapped =
                data.map((MapFunction<String, WebTraffic>) s -> WebTraffic.fromString(s))
                        .filter(wt -> wt.UserCountry.equals("BR"));


        //1.total number of clicks on every website in separate file
/*        mapped.map((MapFunction<WebTraffic, Tuple2<String, Integer>>) webTraffic -> new Tuple2<>(webTraffic.WebSite, 1))
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1500)))
                .sum(1);
                //.print();

/*
        //DataStream<Tuple2<String, Integer>> a =
        mapped.keyBy(wt -> wt.WebSite)
                .sum("TimeSpent")
                .map(new MapFunction<WebTraffic, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(WebTraffic webTraffic) throws Exception {
                        return new Tuple2<>(webTraffic.WebSite, webTraffic.TimeSpent);
                    }
                })
                .print();*/

        //2.  the website with maximum number of clicks in separate file
       /* mapped
                .map(new MapFunction<WebTraffic, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(WebTraffic webTraffic) throws Exception {
                        return new Tuple2<>(webTraffic.WebSite, 1);
                    }
                })
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1500)))
                .sum(1)
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1500)))
                .maxBy(1)
                .print();*/


        //c.) Calculate number of distinct users on every website in separate file.
        mapped
                .keyBy(0)
                .flatMap(new RichFlatMapFunction<WebTraffic, Tuple2<String, Integer>>() {
                    private ListState<String> userIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userIdState = getRuntimeContext().getListState(
                                new ListStateDescriptor<String>("userIdState", BasicTypeInfo.STRING_TYPE_INFO)
                        );
                    }

                    @Override
                    public void flatMap(WebTraffic webTraffic, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        //whatever not going to use
                        userIdState.add(webTraffic.Id);
                    }
                }).print();

        // d.) Calculate the average time spent on website by users.
//        mapped
//                .map(new MapFunction<WebTraffic, Tuple3<String, Integer, Integer>>() {
//                    @Override
//                    public Tuple3<String, Integer, Integer> map(WebTraffic webTraffic) throws Exception {
//                        return new Tuple3<>(webTraffic.WebSite, webTraffic.TimeSpent,1);
//                    }
//                })
//                .keyBy(0)
//                .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
//                    @Override
//                    public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current, Tuple3<String, Integer, Integer> pre) throws Exception {
//                        return new Tuple3<>(pre.f0, pre.f1+current.f1, pre.f2+current.f2);
//                    }
//                })
//                .map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
//                         @Override
//                         public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> val) throws Exception {
//                             return new Tuple2<>(val.f0, val.f1*1.0/ val.f2);
//                         }
//                     })
//                .print();


        env.execute("Udemy example2");
    }

    public static class CabRide {

        public CabRide() {
        }

        //#cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count
        public String Id;
        public String NumberPlate;
        public String Type;
        public String DriverName;
        public String OngoingTrip;
        public String PickLocation;
        public String DropLocation;
        public Integer PassengerCount;


        public static CabRide fromString(String s) {
            CabRide ride = new CabRide();

            String[] tokens = s.split(",");

            ride.Id = tokens[0];
            ride.NumberPlate = tokens[1];
            ride.Type = tokens[2];
            ride.DriverName = tokens[3];
            ride.OngoingTrip = tokens[4];
            ride.PickLocation = tokens[5];
            ride.DropLocation = tokens[6].equals("'null'") ? null : tokens[6];
            ride.PassengerCount = tokens[7].equals("'null'") ? 0 : Integer.parseInt(tokens[7]);

            return ride;
        }

        @Override
        public int hashCode() {
            return super.hashCode() + this.Id.hashCode() + this.NumberPlate.hashCode();
        }

        @Override
        public String toString() {
            return this.Id + " " +
                    this.NumberPlate + " " +
                    this.DriverName + " " +
                    this.PickLocation + " " +
                    this.DropLocation + " ";
        }
    }

    public static class WebTraffic {

        public WebTraffic() {
        }

        // #user_id,network_name,user_IP,user_country,website, Time spent before next click
        public String Id;
        public String NetworkName;
        public String UserIP;
        public String UserCountry;
        public String WebSite;
        public Integer TimeSpent;


        public static WebTraffic fromString(String s) {
            WebTraffic ride = new WebTraffic();

            String[] tokens = s.split(",");

            ride.Id = tokens[0];
            ride.NetworkName = tokens[1];
            ride.UserIP = tokens[2];
            ride.UserCountry = tokens[3];
            ride.WebSite = tokens[4];
            ride.TimeSpent = Integer.parseInt(tokens[5]);
            return ride;
        }

        @Override
        public int hashCode() {
            return super.hashCode() + this.Id.hashCode() + this.TimeSpent.hashCode();
        }

        @Override
        public String toString() {
            return this.Id + " " +
                    this.NetworkName + " " +
                    this.UserIP + " " +
                    this.UserCountry + " " +
                    this.WebSite + " ";
        }
    }

    //region State Examples
    public static void simpleStateFulStreamExample(StreamExecutionEnvironment env) throws Exception {

        Random random = new Random();
        DataStream<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6);


        DataStream<KeyValue> outStream = data
                .map(row -> new KeyValue("testing " + row, row))
                .keyBy(row -> row.getKey())
                .process(new StatefulProcess()).name("stateful_process").uid("stateful_process");
        //.returns(TypeInformation.of(new TypeHint<KeyValue<Integer>>(){}));

        //.keyBy(row -> row.getKey())
        //.flatMap(new StatefulMapTest()).name("stateful_map_test").uid("stateful_map_test");

        outStream.print();
        env.execute("SimpleStateFulStreamExample Job");
    }

    /**
     * We are going to use stateless operator to do stateful operation with the help of external type of state, even
     * when we can achieve the same using window and sum operation
     * This method will read a text file with key and value and emit a sum after 5 keys of same type and result will be
     * non-deterministic because of the arrival of keys isn't guaranteed
     *
     * @param env
     * @throws Exception
     */
    private static void SumByStatelessOperatorsUsingValueState(StreamExecutionEnvironment env) throws Exception {

        DataStream<String> data = ReadTextFile(env, "src/main/resources/wc1.txt");

        DataStream<Float> sumBy5Elements =
                data
                        .map(new MapFunction<String, Tuple2<Integer, Float>>() {
                            @Override
                            public Tuple2<Integer, Float> map(String s) throws Exception {
                                String[] tokens = s.split(",");
                                return new Tuple2<>(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                            }
                        })
                        .keyBy(0)
                        .flatMap(new RichFlatMapFunction<Tuple2<Integer, Float>, Float>() {
                            ValueState<Integer> countValueState;
                            ValueState<Float> sumValueState;

                            @Override
                            public void flatMap(Tuple2<Integer, Float> value, Collector<Float> collector) throws Exception {

                                Integer count = countValueState.value() != null ? countValueState.value() : 0;
                                Float sum = sumValueState.value() != null ? sumValueState.value() : 0F;

                                if (count + 1 == 5) {
                                    collector.collect(sum + value.f1);
                                    sumValueState.clear();
                                    countValueState.clear();

                                } else {
                                    //NDB should be consistent with default values by giving us 0 on first read
                                    //It is consistent as we are using defaultvalue method for null values
                                    countValueState.update(count + 1);
                                    sumValueState.update(sum + value.f1);
                                }
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Integer>("countValueState", BasicTypeInfo.INT_TYPE_INFO));

                                sumValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Float>("sumValueState", BasicTypeInfo.FLOAT_TYPE_INFO));

                            }
                        });


        sumBy5Elements.print();

        env.execute("ListStateExample");
    }

    private static void SumByStatelessOperatorsUsingListState(StreamExecutionEnvironment env) throws Exception {

        DataStream<String> data = ReadTextFile(env, "src/main/resources/wc1.txt");

        DataStream<Tuple2<Integer, Float>> sumBy5Elements =
                data
                        .map(new MapFunction<String, Tuple2<Integer, Float>>() {
                            @Override
                            public Tuple2<Integer, Float> map(String s) throws Exception {
                                String[] tokens = s.split(",");
                                return new Tuple2<>(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                            }
                        })
                        .keyBy(0)
                        .flatMap(new RichFlatMapFunction<Tuple2<Integer, Float>, Tuple2<Integer, Float>>() {
                            ValueState<Integer> countValueState;
                            ListState<Float> valuesListState;

                            @Override
                            public void flatMap(Tuple2<Integer, Float> value, Collector<Tuple2<Integer, Float>> collector) throws Exception {

                                Integer count = countValueState.value() != null ? countValueState.value() : 0;


                                if (count + 1 == 5) {
                                    Float sum = 0F;
                                    for (Float f : valuesListState.get()) {
                                        sum += f;
                                    }
                                    collector.collect(new Tuple2<>(value.f0, sum + value.f1));
                                    valuesListState.clear();
                                    countValueState.clear();
                                }else if (count == 3){
                                    valuesListState.addAll(Arrays.asList(1.0f,2.2f));
                                }
                                else {
                                    //NDB should be consistent with default values by giving us 0 on first read
                                    countValueState.update(count + 1);
                                    valuesListState.add(value.f1);
                                }
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Integer>("countValueState", BasicTypeInfo.INT_TYPE_INFO));

                                valuesListState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<Float>("valuesListState", BasicTypeInfo.FLOAT_TYPE_INFO)
                                );
                            }
                        });


        sumBy5Elements.print();

        env.execute("ListStateExample");
    }

    private static void SumByStatelessOperatorsUsingReducingState(StreamExecutionEnvironment env) throws Exception {

        DataStream<String> data = ReadTextFile(env, "src/main/resources/wc1.txt");

        DataStream<Tuple2<Integer, Float>> sumBy5Elements =
                data
                        .map(new MapFunction<String, Tuple2<Integer, Float>>() {
                            @Override
                            public Tuple2<Integer, Float> map(String s) throws Exception {
                                String[] tokens = s.split(",");
                                return new Tuple2<>(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                            }
                        })
                        .keyBy(0)
                        .flatMap(new RichFlatMapFunction<Tuple2<Integer, Float>, Tuple2<Integer, Float>>() {
                            ValueState<Integer> countValueState;
                            ReducingState<Float> sumReducingState;

                            @Override
                            public void flatMap(Tuple2<Integer, Float> value, Collector<Tuple2<Integer, Float>> collector) throws Exception {

                                Integer count = 1 + (countValueState.value() != null ? countValueState.value() : 0);

                                countValueState.update(count);
                                sumReducingState.add(value.f1);

                                if (count == 5) {
                                    collector.collect(new Tuple2<>(value.f0, sumReducingState.get()));
                                    sumReducingState.clear();
                                    countValueState.clear();
                                }
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Integer>("countValueState", BasicTypeInfo.INT_TYPE_INFO));

                                sumReducingState = getRuntimeContext().getReducingState(
                                        new ReducingStateDescriptor<Float>("sumReducingState",
                                                new ReduceFunction<Float>() {
                                                    @Override
                                                    public Float reduce(Float aFloat, Float t1) throws Exception {
                                                        return aFloat + t1;
                                                    }
                                                },
                                                BasicTypeInfo.FLOAT_TYPE_INFO)
                                );
                            }
                        });


        sumBy5Elements.print();

        env.execute("ListStateExample");
    }

    private static void SumByStatelessOperatorsUsingAggregateState(StreamExecutionEnvironment env) throws Exception {

        DataStream<String> data = ReadTextFile(env, "src/main/resources/wc1.txt");

        DataStream<Tuple2<Integer, Float>> sumBy5Elements =
                data
                        .map(new MapFunction<String, Tuple2<Integer, Float>>() {
                            @Override
                            public Tuple2<Integer, Float> map(String s) throws Exception {
                                String[] tokens = s.split(",");
                                return new Tuple2<>(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                            }
                        })
                        .keyBy(0)
                        .flatMap(new RichFlatMapFunction<Tuple2<Integer, Float>, Tuple2<Integer, Float>>() {
                            ValueState<Integer> countValueState;
                            AggregatingState<Float, Float> sumAggregateState; //could have used reducing but for running the aggregate code

                            @Override
                            public void flatMap(Tuple2<Integer, Float> value, Collector<Tuple2<Integer, Float>> collector) throws Exception {

                                Integer count = 1 + (countValueState.value() != null ? countValueState.value() : 0);

                                countValueState.update(count);
                                sumAggregateState.add(value.f1);

                                if (count == 5) {
                                    collector.collect(new Tuple2<>(value.f0, sumAggregateState.get()));
                                    sumAggregateState.clear();
                                    countValueState.clear();
                                }
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Integer>("countValueState", BasicTypeInfo.INT_TYPE_INFO));

                                sumAggregateState = getRuntimeContext().getAggregatingState(
                                        new AggregatingStateDescriptor<Float, Float, Float>("sumAggregateState",
                                                new AggregateFunction<Float, Float, Float>() {
                                                    @Override
                                                    public Float createAccumulator() {
                                                        return 0.0F;
                                                    }

                                                    @Override
                                                    public Float add(Float value, Float accumulator) {
                                                        return value + accumulator;
                                                    }

                                                    @Override
                                                    public Float getResult(Float accumulator) {
                                                        return accumulator;
                                                    }

                                                    @Override
                                                    public Float merge(Float accumulatorA, Float accumulatorB) {
                                                        return add(accumulatorA, accumulatorB);
                                                    }
                                                },
                                                BasicTypeInfo.FLOAT_TYPE_INFO)
                                );


                            }
                        });


        sumBy5Elements.print();

        env.execute("ListStateExample");
    }

    private static void SumByStatelessOperatorsUsingMapState(StreamExecutionEnvironment env) throws Exception {

        DataStream<String> data = ReadTextFile(env, "src/main/resources/wc1.txt");

        DataStream<Tuple2<Integer, Float>> sumByMapState =
                data
                        .map(new MapFunction<String, Tuple2<Integer, Float>>() {
                            @Override
                            public Tuple2<Integer, Float> map(String s) throws Exception {
                                String[] tokens = s.split(",");
                                return new Tuple2<>(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                            }
                        })
                        .keyBy(0)
                        .flatMap(new RichFlatMapFunction<Tuple2<Integer, Float>, Tuple2<Integer, Float>>() {
                            ValueState<Integer> countValueState;
                            MapState<Integer, Float> sumMapState;

                            @Override
                            public void flatMap(Tuple2<Integer, Float> value, Collector<Tuple2<Integer, Float>> collector) throws Exception {

                                if (sumMapState.contains(value.f0)) {
                                    Float sum = sumMapState.get(value.f0);
                                    sum += value.f1;
                                    collector.collect(new Tuple2<>(value.f0, sum));
                                    sumMapState.put(value.f0, sum);

                                } else {
                                    sumMapState.put(value.f0, value.f1);
                                }
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<Integer>("countValueState", BasicTypeInfo.INT_TYPE_INFO));

                                sumMapState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<Integer, Float>("sumMapState",
                                                BasicTypeInfo.INT_TYPE_INFO,
                                                BasicTypeInfo.FLOAT_TYPE_INFO)
                                );
                            }
                        });


        sumByMapState.print();

        env.execute("Map State Example");
    }


    private static void CountTriggerWindowExample(StreamExecutionEnvironment env) throws Exception {

        DataStream<String> data = ReadTextFile(env, "src/main/resources/wc1.txt");

        DataStream<Tuple2<Integer, Float>> sumBy5Elements =
                data.map(new MapFunction<String, Tuple2<Integer, Float>>() {
                    @Override
                    public Tuple2<Integer, Float> map(String s) throws Exception {
                        String[] tokens = s.split(",");
                        return new Tuple2<>(Integer.parseInt(tokens[0]), Float.parseFloat(tokens[1]));
                    }
                })
                        .keyBy(0)
                        .window(GlobalWindows.create())
                        .trigger(CountTrigger.of(5))
                        .reduce(new ReduceFunction<Tuple2<Integer, Float>>() {
                            @Override
                            public Tuple2<Integer, Float> reduce(Tuple2<Integer, Float> current, Tuple2<Integer, Float> pre) throws Exception {
                                return new Tuple2<>(current.f0, pre.f1 + current.f1);
                            }
                        });

        sumBy5Elements.print();

        env.execute("ListStateExample");
    }

    private static void WordCountUsingMapStateUntilThree(StreamExecutionEnvironment env) throws Exception {

        //open socket with nc -l 9999 before running the program
        DataStream<String> data = env.socketTextStream("localhost", 9999);

        DataStream<Tuple2<String, Integer>> count =
                data.keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String s) throws Exception {
                        return s;
                    }
                })

                        .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {

                            ValueState<Integer> countValueState;
                            ValueState<Integer> countValueState1;
                            ValueState<Integer> countValueState2;
                            MapState<String, Integer> sumMapState;
                            ListState<Integer> lotOfValuesState;

                            @Override
                            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

                                Integer sum = 1;
                                if (sumMapState.contains(s)) {
                                    sum = sumMapState.get(s);
                                    sum += 1;
                                    countValueState.update(sum);
                                    countValueState2.update(sum);
                                    countValueState1.update(sum);
                                    sumMapState.put(s,sum);

                                } else {
                                    sumMapState.put(s, 1);
                                    countValueState.update(1);
                                    countValueState2.update(1);
                                    countValueState1.update(1);
                                }

                                if (s.equals("crash") && countValueState.value() >= 2) {
                                    throw new FlinkRuntimeException("Ahah");
                                }
                                else if(s.startsWith("insert")){
                                    List<Integer> list = new ArrayList<>();

                                    for(int i=0; i < s.length();i++)
                                    {
                                        list.add(i);
                                    }

                                    lotOfValuesState.addAll(list);
                                }
                                collector.collect(new Tuple2<>(s,sum));
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {

                                countValueState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("countValueState", BasicTypeInfo.INT_TYPE_INFO));

                                countValueState1 = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("countValueState1", BasicTypeInfo.INT_TYPE_INFO));
                                countValueState2 = getRuntimeContext().getState(
                                        new ValueStateDescriptor<>("countValueState2", BasicTypeInfo.INT_TYPE_INFO));

                                sumMapState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<>("sumMapState",
                                                BasicTypeInfo.STRING_TYPE_INFO,
                                                BasicTypeInfo.INT_TYPE_INFO));

                                lotOfValuesState = getRuntimeContext().getListState(
                                        new ListStateDescriptor<Integer>("lotOfValuesState",
                                                BasicTypeInfo.INT_TYPE_INFO));

                            }
                        });

        //.sum(1).uid("KeyBy-sum-id");

        count.print();

        env.execute("Word count example execution");

    }
    //endregion

    private static void KeyByFun(StreamExecutionEnvironment env) throws Exception {

        File file = new File("src/main/resources/cab-flink.txt");
        String absolutePath = file.getAbsolutePath();

        DataStream<String> data = env.readTextFile(absolutePath);

        DataStream<CabRide> mapped =
                data.map((MapFunction<String, CabRide>) s -> CabRide.fromString(s));

        SingleOutputStreamOperator<Tuple2<CabRide, Integer>> result1 =
                mapped
                        .map(new MapFunction<CabRide, Tuple2<CabRide, Integer>>() {
                            @Override
                            public Tuple2<CabRide, Integer> map(CabRide cabRide) throws Exception {
                                return new Tuple2<>(cabRide, 1);
                            }
                        })
                        .keyBy(new KeySelector<Tuple2<CabRide, Integer>, Integer>() {
                            @Override
                            public Integer getKey(Tuple2<CabRide, Integer> cabRideIntegerTuple2) throws Exception {
                                return cabRideIntegerTuple2.f0.Id.hashCode() % 8;
                            }
                        })
                        .sum(1);


        result1.print();
        //map, filter, reduce, groupby, sum, maxby

        //2.) Average number of passengers from each pickup location.  | average =  total no. of passengers from a location / no. of trips from that location.
//		mapped.filter(ride -> ride.PickLocation != null)
//				.keyBy("PickLocation")
//				.map((MapFunction<CabRide, Tuple3<CabRide,Integer, Integer>>) ride-> new Tuple3<>(ride,1, ride.PassengerCount))
//				.keyBy("PickLocation")
//				.reduce((ReduceFunction<Tuple3<CabRide, Integer, Integer>>) (current, pre)
//						-> new Tuple3<>(current.f0, current.f1+pre.f1, current.f2+pre.f2));
//				//.keyBy("PickLocation")
//				.//fold(Tup)
//
//		mapped.filter(ride -> ride.DriverName != null)
//				.keyBy("DriverName")
//				.map((MapFunction<CabRide, Tuple3<CabRide,Integer, Integer>>) ride-> new Tuple3<>(ride,1, ride.PassengerCount))
//				.keyBy("DriverName")
//				.sum(2)
//				.keyBy("DriverName")
//				.fold(new Tuple2<String, Double>("", 0), new FoldFunction<Tuple3<CabRide, Integer, Integer>, Tuple2<String, Double>>() {
//					@Override
//					public Tuple2<String, Double> fold(Tuple2<String, Double> defalutVal, Tuple3<CabRide, Integer, Integer> current) throws Exception {
//						return new Tuple2<>(current.f0.DriverName, current.f2*1.0/current.f1 );
//					}
//				});

        env.execute("Udemy example");
    }

    private static DataStream<String> ReadTextFile(StreamExecutionEnvironment env, String filePath) {
        File file = new File(filePath);
        String absolutePath = file.getAbsolutePath();

        return env.readTextFile(absolutePath);
    }

    static class StatefulProcess extends KeyedProcessFunction<String, KeyValue, KeyValue> {
        ValueState<Integer> processedInt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            processedInt = getRuntimeContext().getState(new ValueStateDescriptor<>("processedInt", Integer.class));
        }

        @Override
        public void processElement(KeyValue keyValue, Context context, Collector<KeyValue> collector) throws Exception {
            try {
                processedInt.update(keyValue.getValue());
                collector.collect(keyValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}

