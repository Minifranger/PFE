package ensai;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.util.concurrent.TimeoutException;

import org.apache.flink.streaming.api.datastream.DataStream;

import com.rabbitmq.client.Channel;


public class Send {
	private final static String QUEUE_NAME = "hello";

	public static void main(String[] argv)
			throws java.io.IOException, TimeoutException {
		
		
		// create a connection to the server
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		
		//declare a queue for us to send to; then we can publish a message to the queue
		channel.queueDeclare(QUEUE_NAME, true, false, false, null);
	    String message = "Hello world !";
	    channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
	    System.out.println(" [x] Sent '" + message + "'");
	    
	    //Lastly, we close the channel and the connection
	    channel.close();
	    connection.close();
	}
}

