package ensai;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class SendRMQ {

	
	final DataStream<String> stream = ...;

			final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			    .setHost("localhost")
			    .setPort(5000)
			    .build();
			    
			stream.addSink(new RMQSink<String>(
			    connectionConfig,            // config for the RabbitMQ connection
			    "hello",                 // name of the RabbitMQ queue to send messages to
			    new SimpleStringSchema())); 
			
			
}
