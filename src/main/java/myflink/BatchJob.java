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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import javax.lang.model.element.ElementVisitor;
import java.io.File;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);

		//WordCount(env);
		//JoinsExample(env);
		groupByExample(env);
		return;

		//env.execute("Flink Batch Java API Skeleton");

//		 input.filter(new FilterFunction<String>() {
//			 @Override
//			 public boolean filter(String s) throws Exception {
//				 return s.startsWith("M");
//			 }
//		 });

//		DataSet<Tuple2<String, Integer>> tokenized = filteredM.map(new MapFunction<String, Tuple2<String, Integer>>() {
//
//			@Override
//			public Tuple2<String, Integer> map(String s) throws Exception {
//				return Tuple2.of(s,1);
//			}
//		});

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */

		// execute program

	}

	private static void JoinsExample(ExecutionEnvironment env) throws Exception {
		File file = new File("src/main/resources/IDName.txt");
		String absolutePath = file.getAbsolutePath();

		DataSet<Tuple2<Integer,String>> personSet= env.readTextFile(absolutePath)
				.map(new MapFunction<String, Tuple2< Integer, String>>() {
					@Override
					public Tuple2<Integer, String> map(String s) throws Exception {
						String[] words = s.split(",");
						return  new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
					}
				});


		File file2 = new File("src/main/resources/IDOther.txt");
		String absolutePath2 = file2.getAbsolutePath();
		DataSet<Tuple2<Integer, String>> locationSet= env.readTextFile(absolutePath2)
				.map(new MapFunction<String, Tuple2< Integer, String>>() {
						 @Override
						 public Tuple2<Integer, String> map(String s) throws Exception {
								 String[] words = s.split(",");
								 return  new Tuple2<Integer, String>(Integer.parseInt(words[0]), words[1]);
						 }
					 });


		DataSet<Tuple3<Integer, String, String>> joined =
				personSet.join(locationSet, JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES)
				.where(0)
				.equalTo(0)
				.with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>,
						Tuple3<Integer, String, String>>) (personTuple, locationTuple)
						-> new Tuple3<>(personTuple.f0, personTuple.f1, locationTuple == null? "NULL" :locationTuple.f1));

		joined.print();


	}


	private static  void groupByExample(ExecutionEnvironment env) throws Exception {

		File file = new File("src/main/resources/wc1.txt");
		String absolutePath = file.getAbsolutePath();


		DataSource<String> data = env.readTextFile(absolutePath);

		AggregateOperator<Tuple3<Integer, Integer, Integer>> mapped =
				data.map(new MapFunction<String, Tuple3<Integer, Integer, Integer>>() {
					@Override
					public Tuple3<Integer, Integer, Integer> map(String s) throws Exception {
						String[] tokens= s.split(",");
						return new Tuple3<>(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]),1);
					}
				})
				.groupBy(0,1)
				.sum(2);
				//.aggregate(new AggregateFunction<Tuple2<Integer, Integer>, AverageAccumulator, Tuple3<Integer, Integer, Integer>>() {
		mapped.print();
	}

	// the accumulator, which holds the state of the in-flight aggregate
	public static class AverageAccumulator {
		long driveID;
		long weekNo;
		long count;

		public AverageAccumulator(){
			driveID=0;
			weekNo=0;
			count=0;
		}
	}


	/**
	 * Word count exmample to count the names in the file only*/
	private static void WordCount(ExecutionEnvironment env) throws Exception {
		File file = new File("src/main/resources/wc.txt");
		String absolutePath = file.getAbsolutePath();

		DataSet<String> input= env.readTextFile(absolutePath);

		DataSet<String> filteredM =  input.filter(s -> s.startsWith("M"));
		DataSet<Tuple2<String, Integer>> tokenized = filteredM.map(s -> Tuple2.of(s,1))
				.returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
							@Override
							public TypeInformation<Tuple2<String, Integer>> getTypeInfo() {
								return super.getTypeInfo();
							}
						}));

		DataSet<Tuple2<String, Integer>> counts =  tokenized.groupBy(0).sum(1);

		counts.print(); //Print triggers the exuection so we don't need to call execute explicitly
	}


}
