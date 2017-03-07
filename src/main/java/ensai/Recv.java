package ensai;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


public class Recv {
	  private final static String QUEUE_NAME = "voila";

	  public static void main(String[] argv)
	      throws java.io.IOException,
	             java.lang.InterruptedException, TimeoutException {

	    ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();

	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	    
	    //we declare the queue here, as well. 
	    //Because we might start the receiver before the sender, we want to make sure the queue exists 
	    //before we try to consume messages from it
	    
	    //an object that will buffer the messages until we're ready to use them. That is what a DefaultConsumer subclass does
	    Consumer consumer = new DefaultConsumer(channel) {
	        @Override
	        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
	            throws IOException {
	          String message = new String(body, "UTF-8");
	          System.out.println(" [x] Received '" + message + "'");
	        }
	      };
	      channel.basicConsume(QUEUE_NAME, true, consumer);
	    
	    
	    }
	}
