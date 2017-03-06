package ensai;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Collector;

import ensai.StreamRabbitMQ.InputAnalyze;

//TODO : à finir

public class OutputData {
	
	static int numA = 0;
	static int numT = 0;

	public static void main(String[] args) {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder().setHost("localhost")
				.setPort(5672).setUserName("guest").setPassword("guest").setVirtualHost("/").build();




	}
	
	// Setter qui incremente le numero d'anomalie en sortie pour des IRI uniques
	public static void setNumA(){
		numA = numA + 1;
	}
	
	// Setter qui incremente le numero de timestamp en sortie pour des IRI uniques
	public static void setNumT(){
		numT = numT + 1;
	}
	
	// Le RDF en sortie doit être généré selon le modèle suivant : 
	//	yourNamespace:Anomaly_1 rdf:type ar:Anomaly.
	//	yourNamespace:Anomaly_1 ar:hasProbabilityOfObservedAbnormalSequence	"0.8"^^xsd:float.
	//	yourNamespace:Anomaly_1 ar:hasTimestamp debs:Timestamp_4.
	//	yourNamespace:Anomaly_1 ar:inAbnormalDimension wmm:_3.
	//	yourNamespace:Anomaly_1 i40:machine wmm:MoldingMachine_1.
	//	yourNamespace:Timestamp_4 rdf:type IoTCore:Timestamp.
	//	yourNamespace:Timestamp_4 IoTCore:valueLiteral "2017-01-17T00:15:00".
	public static class sortieTransfo implements FlatMapFunction<Tuple5<Integer, Integer, Float, String, Float>, String> {

		@Override
		public void flatMap(Tuple5<Integer, Integer, Float, String, Float> avant,
				Collector<String> apres) throws Exception {
			String numMachine = avant.f0.toString();
			String numSensor = avant.f1.toString();
			String valeur = avant.f2.toString();
			String timestamp = avant.f3.toString();
			String probTrans = avant.f4.toString();	

			String numAno = Integer.toString(numA);
			String numTsp = Integer.toString(numT);
			
			String l1 = "<http://yourNamespace/debs2017#Anomaly_" + numAno + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#Anomaly> ."; 
			String l2 = "<http://yourNamespace/debs2017#Anomaly_" + numAno + "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasProbabilityOfObservedAbnormalSequence> \"" + probTrans + "\"^^<http://www.w3.org/2001/XMLSchema#float> ."; 
			String l3 = "<http://yourNamespace/debs2017#Anomaly_" + numAno + "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasTimestamp> <http://project-hobbit.eu/resources/debs2017#Timestamp_" + numTsp + "> .";
			String l4 = "<http://yourNamespace/debs2017#Anomaly_" + numAno + "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#inAbnormalDimension> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#" + numSensor + "> .";
			String l5 = "<http://yourNamespace/debs2017#Anomaly_" + numAno + "> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#MoldingMachine_" + numMachine + ">";
			String l6 = "<http://yourNamespace/debs2017#Timestamp_" + numTsp + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/IoTCore#Timestamp> .";
			String l7 = "<http://yourNamespace/debs2017#Timestamp_" + numTsp + "> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> \"" + timestamp + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .";
			
			
			apres.collect(l1);
			apres.collect(l2);
			apres.collect(l3);
			apres.collect(l4);
			apres.collect(l5);
			apres.collect(l6);
			apres.collect(l7);
		}
	}


}
