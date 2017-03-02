package ensai;


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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import ensai.SocketTextStreamWordCount.LineSplitter;

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
 * Usage:
 * <code>SocketTextStreamWordCount &lt;hostname&gt; &lt;port&gt;</code>
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
	//	Program
	//

	public static void main(String[] args) throws Exception {

	
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
		    .setHost("localhost")	
		    .setPort(5672)
		    .setUserName("guest")
		    .setPassword("guest")
		    .setVirtualHost("/")
		    .build();
		    
		final DataStream<String> stream = env
		    .addSource(new RMQSource<String>(
		        connectionConfig,            // config for the RabbitMQ connection
		        "coucou",                 // name of the RabbitMQ queue to consume
		        true,                        // use correlation ids; can be false if only at-least-once is required
		        new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
		    .setParallelism(1);  

		DataStream<Tuple3<String, String, String>> res  = 	stream.flatMap(new LineSplitter());

		res.print();

		// execute program
		env.execute("Java WordCount from SocketTextStream Example");
	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple3<String, String, String>> {

		@Override
		public void flatMap(String value, Collector<Tuple3<String, String, String>> out) {
			
			// normalize and split the line
			String[] tokens = value.toLowerCase().split(" ");
			tokens[0] = tokens[0].split("#")[1];
			tokens[1] = tokens[1].split("#")[1];
			String[] tab  = tokens[2].split("\\^\\^");
			if (tab.length == 1){
				tokens[2] = tab[0].split("#")[1];
			}
			else{
				tokens[2] = tab[0];
			}
			
			out.collect(new Tuple3<String, String,String>(tokens[0], tokens[1], tokens[2]));
			
			// emit the pairs
//			for (String token : tokens) {
//				if (token.length() > 0) {
//					out.collect(new Tuple3<String, String,String>(token, 1));
//				}
//			}
			
		}
	}	
}
