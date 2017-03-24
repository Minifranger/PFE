package ensai;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import com.rabbitmq.client.Channel;

public class Send {
	
	private final static String QUEUE_NAME = "coucou";
	static List<byte[]> intermediaire = new ArrayList<byte[]>();
	static List<String> intermediaire0 = new ArrayList<String>();

	public static void main(String[] argv) throws java.io.IOException, TimeoutException, InterruptedException {

		int waitTime = 1000;
		int i = 1;
		// create a connection to the server
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		//		File f = new File("./ressources/molding_machine_10M.nt");
		//		File f = new File("./ressources/molding_machine_10M.nt");

		String fileName = "./ressources/molding_machine_1M.nt";
		try {
			intermediaire0 = Files.readAllLines(Paths.get(fileName));

			//stream.forEach(System.out::println);
			//intermediaire.add(stream.forEach(intermediaire.add(e)););
		} catch (IOException e) {
			e.printStackTrace();
		}

		// declare a queue for us to send to; then we can publish a message to
		// the queue
		channel.queueDeclare(QUEUE_NAME, true, false, false, null);
		//		try{
		//			System.out.println("Waaaaaaaaaaaaait");
		//			Thread.sleep(waitTime*1000);
		//		}catch (InterruptedException e) {}

		for(int k = 0; k < intermediaire0.size(); k++){
			intermediaire.add(intermediaire0.get(k).getBytes());
		}

		int c = 0;
		long t1 = System.currentTimeMillis();
		for(int h = 0; h < intermediaire.size(); h++){

			if (c%944 == 1 && c!=1){
				try{
					Thread.sleep(waitTime);
				}catch (InterruptedException e) {}
			}
			channel.basicPublish("", QUEUE_NAME, null, intermediaire.get(h)); 

			c++;
		}

		channel.close();
		connection.close();
	}
}
