package ensai;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * This example shows an implementation of WordCount with data from a text
 * socket. To run the example make sure that the service providing the text data
 * is already up and running.
 * 
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 * 
 * 
 * <p>
 * Usage: <code>SocketTextStreamWordCount &lt;hostname&gt; &lt;port&gt;</code>
 * <br>
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use StreamExecutionEnvironment.socketTextStream
 * <li>write a simple Flink program
 * <li>write and use user-defined functions
 * </ul>
 * 
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 */
public class StreamRabbitMQ {
	//
	// Program
	//

	public static int currentObsGroup;
	public static Map<Integer, Integer> mapObsSensors = new HashMap<Integer, Integer>();
	public static String currentTimestamp;
	public static int currentMachine;

	public static void main(String[] args) throws Exception {

		List<Integer> listSensors = new ArrayList<Integer>();
		listSensors.add(3);
		listSensors.add(17);
		listSensors.add(19);
		listSensors.add(20);
		listSensors.add(23);


		String currentTimestamp = null;
		int currentMachine = 0;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder().setHost("localhost")
				.setPort(5672).setUserName("guest").setPassword("guest").setVirtualHost("/").build();

		final DataStream<String> stream = env
				.addSource(new RMQSource<String>(connectionConfig, // config for
																	// the
																	// RabbitMQ
																	// connection
						"coucou", // name of the RabbitMQ queue to consume
						true, // use correlation ids; can be false if only
								// at-least-once is required
						new SimpleStringSchema())) // deserialization schema to
													// turn messages into Java
													// objects
				.setParallelism(1);

		// stream.print();

		DataStream<Tuple3<String, String, String>> res = stream.map(new LineSplitter());

		//res.print();

		DataStream<Tuple4<Integer, Integer, Float, String>> st = res
				.flatMap(new InputAnalyze(listSensors));

		 st.print();

		// execute program
		env.execute("Java WordCount from SocketTextStream Example");
	}

	//
	// User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" (Tuple2<String,
	 * Integer>).
	 */
	public static final class LineSplitter implements MapFunction<String, Tuple3<String, String, String>> {

		@Override
		public Tuple3<String, String, String> map(String value) throws Exception {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split(" ");
			tokens[0] = tokens[0].split("#")[1];
			tokens[1] = tokens[1].split("#")[1];
			String[] tab = tokens[2].split("\\^\\^");
			if (tab.length == 1) {
				tokens[2] = tab[0].split("#")[1];
			} else {
				tokens[2] = tab[0];
			}
			return new Tuple3<String, String, String>(tokens[0], tokens[1], tokens[2]);
		}
	}

	public static final class InputAnalyze
			implements FlatMapFunction<Tuple3<String, String, String>, Tuple4<Integer, Integer, Float, String>> {

		List<Integer> listSensors;
	

		public InputAnalyze(List<Integer> listSensors) {
			this.listSensors = listSensors;

		}

		@Override
		public void flatMap(Tuple3<String, String, String> value,
				Collector<Tuple4<Integer, Integer, Float, String>> out) throws Exception {

			switch (value.f1) {
			
			case "machine>":
				StreamRabbitMQ.currentMachine = Integer.parseInt(value.f2.split("\\_|>")[1]);
				break;

			case "observationresulttime>":
				StreamRabbitMQ.currentObsGroup = Integer.parseInt(value.f0.split("\\_|>")[1]);
//				System.out.println("Observation group : " + StreamRabbitMQ.currentObsGroup);
				break;

			case "observedproperty>":

				// System.out.println("Numéro de capteur : " +
				// Integer.parseInt(value.f2.split("\\_|>")[1]));
				// System.out.println("Numéro d'observation : " +
				// Integer.parseInt(value.f0.split("\\_|>")[1]));

				int sensorNb = Integer.parseInt(value.f2.split("\\_|>")[1]);
				int obsNb = Integer.parseInt(value.f0.split("\\_|>")[1]);

//				System.out.println(listSensors.contains(sensorNb));
//				System.out.println(listSensors);
//				System.out.println(sensorNb);

				if (listSensors.contains(sensorNb)) {
					StreamRabbitMQ.mapObsSensors.put(obsNb, sensorNb);
//					System.out.println("Numéro d'observation enregisté");
				} else {
//					System.out.println("Ce capteur ne nous interesse pas !!");
				}

				break;

			case "valueliteral>":

				String[] tab = value.f0.split("\\_|>");

				if (tab[0].split("\\_")[0].equals("timestamp")) {
					StreamRabbitMQ.currentTimestamp = value.f2;
//					System.out.println("Timestamp mis à jour");
				}

				else if (tab[0].equals("value") && StreamRabbitMQ.mapObsSensors.containsKey(Integer.parseInt(tab[1]))) {
//					System.out.println(" ================= > " + "Capteur : "
//							+ StreamRabbitMQ.mapObsSensors.get(Integer.parseInt(tab[1])) + " | Valeur : "
//							+ Float.parseFloat(value.f2.split("\"")[1]));

					out.collect(new Tuple4<Integer, Integer, Float, String>(
							StreamRabbitMQ.currentMachine,
							StreamRabbitMQ.mapObsSensors.get(Integer.parseInt(tab[1])) ,
							Float.parseFloat(value.f2.split("\"")[1]) ,
							StreamRabbitMQ.currentTimestamp));

					StreamRabbitMQ.mapObsSensors.remove(Integer.parseInt(tab[1]));

				}
				break;

			}
		}
	}
}
