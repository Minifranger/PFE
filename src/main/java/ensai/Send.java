package ensai;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;

public class Send {
	private final static String QUEUE_NAME = "coucou";

	public static void main(String[] argv) throws java.io.IOException, TimeoutException {

		int waitTime = 3;
		int i = 1;
		// create a connection to the server
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

//		File f = new File("./ressources/molding_machine_10M.nt");
		File f = new File("./ressources/molding_machine_10M.nt");

		BufferedReader br = new BufferedReader(new FileReader(f));
		String line;
		// declare a queue for us to send to; then we can publish a message to
		// the queue
		channel.queueDeclare(QUEUE_NAME, true, false, false, null);
		while ((line = br.readLine()) != null) {
			
			if (i%945 == 0){
				try{
					System.out.println("Waaaaaaaaaaaaait");
					Thread.sleep(waitTime*1000);
				}catch (InterruptedException e) {}
			}
			
			channel.basicPublish("", QUEUE_NAME, null, line.getBytes());
			System.out.println(" [x] Sent '" + line + "'");
			i++;
		}
		br.close();

		// String message = "Hello world !";

		// Lastly, we close the channel and the connection
		channel.close();
		connection.close();
	}
}
