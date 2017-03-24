package ensai;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;

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

//TODO virer import
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import localKmeans.Cluster;
import localKmeans.KMeans;
import localKmeans.Point;

/**
 * This class represent a solution from ENSAI to the DEBS 2017 challenge.
 * @see http://www.debs2017.org/call-for-grand-challenge-solutions/ 
 * The github solution can be accessed at
 * @see https://github.com/kent930/PFE
 * 
 * <p>
 * To start the main class, you need to :
 *  									 - start Rabbot MQ : invoke-rc.d rabbitmq-server start
 *  									 - start flink 1.2.0 (from flink directory): ./bin/start-local.sh
 *  									 - build the solution jar (from solution projet directory) : mvn clean install -Pbuild-jar
 *  									 - start the main class (from flink directory) : bin/flink run -c ensai.RMQtestKmeans /home/minifranger/ensai_debs/PFE/target/debs-1.0-SNAPSHOT.jar 
 * 										 - start the flink jobmanager output (from flink directory) : tail -f log/flink-*-jobmanager-*.out
 * 										 - start the Send.java class (from Eclipse)
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////// MAIN CLASS ////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

public class RMQtestKmeans {

	/**
	 * Le chemin du fichier metadata qui sert de propreties.
	 * @see Metadata
	 */
	static String cheminConfiguration = "/home/minifranger/ensai_debs/PFE/ressources/metadata";

	//TODO Rajouter javadoc
	public static int currentObsGroup;
	public static Map<Integer, Integer> mapObsSensors = new HashMap<Integer, Integer>();
	public static String currentTimestamp;
	public static String currentMachineType;
	public static int currentMachine;

	/**
	 * Le numéro de l'anomalie dans la sortie RDF. 
	 * @see SortieTransformation
	 */
	public static int numeroAnomalie = 0;

	/**
	 * Le numéro du timestamp dans la sortie RDF. 
	 * @see SortieTransformation
	 */
	public static int numeroTimestamp = 0;

	/**
	 * La taille de la fenêtre sur laquelle appliquer l'algorithme KMean.
	 * @see KMean
	 */
	public static int countWindowSize = 10;

	/**
	 * Le nombre de transitions utilisés pour le calcul d'une probabilité de transition.
	 * @see Markov
	 */
	public static int N = 5;

	/**
	 * Le nombre maximal d'itération de l'algorithme KMean.
	 * @see KMean
	 */
	public static int maxIterationKMean = 50;

	//TODO Rajouter javadoc, changer nom variable et valeur de la variable
	//private final static String QUEUE_NAME2 = "voila";

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////// MAIN METHOD ///////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception {

		//TODO javadoc : expliquer ce bloc metadata et pourquoi !file.exists
		Metadata metadata = new Metadata();

		File file = new File(cheminConfiguration);

		if (!file.exists()){
			metadata.readData();
		}

		/**
		 * Création de la map qui contient la nombre de clusters pour le KMean et le seuil de détection d'anomalies pour chaque capteurs.
		 * @see Metadata 
		 */
		Map<Integer, Tuple2<Integer, Double>> mapSensors_ClustersSeuils = new HashMap<Integer, Tuple2<Integer, Double>>();
		mapSensors_ClustersSeuils = metadata.load();

		/**
		 * Création de l'environnement de streaming de Flink
		 * @see https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/api_concepts.html
		 */
		final StreamExecutionEnvironment environnementStream = StreamExecutionEnvironment.getExecutionEnvironment();

		//TODO javadoc, expliquer demarche, expliquer localhost, port, guest, guest
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder().setHost("localhost")
				.setPort(5672).setUserName("guest").setPassword("guest").setVirtualHost("/").build();

		//TODO Changer coucou dans tout le système, et dans la javadoc en dessous, expliquer  true et simple schema
		/**
		 * Création du datastream grâce à la source RabbitMQ
		 * connectionConfig : Configuration de la connection de RabbitMQ
		 * coucou : le nom de la queue RabbitMQ à consommer
		 * true : 
		 * @see StreamExecutionEnvironment.addSource()
		 */
		final DataStream<String> streamFromRabbitMQ = environnementStream
				.addSource(new RMQSource<String>(connectionConfig, 
						"coucou", 
						true, // use correlation ids; can be false if only at-least-once is required
						new SimpleStringSchema())); // deserialization schema to turn messages into Java objects
		/**
		 * Application de la map LineSplitter
		 * @see LineSplitter
		 */
		DataStream<Tuple3<String, String, String>> streamAfterSplit = streamFromRabbitMQ.flatMap(new LineSplitter1())
				.flatMap(new LineSplitter2())
				.flatMap(new LineSplitter3())
				.flatMap(new LineSplitter4())
				.flatMap(new LineSplitter5());

		/**
		 * Application de la flatMap InputAnalyze
		 * @see InputAnalyze
		 */
		DataStream<Tuple4<Integer, Integer, Float, String>> streamAfterAnalyze = streamAfterSplit.flatMap(new InputAnalyze(mapSensors_ClustersSeuils));

		//streamAfterAnalyze.print();
		/**
		 * Application du keyBy pour séparer le stream selon la valeur du couple (field0, field1) de chaque tuple du stream
		 * Mise en place d'une window glissante de taille countWindowSize. L'apparition de chaque nouveau input trigger la fonction apply.
		 * @see https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/api_concepts.html
		 * 
		 * Application de la fonction KMean
		 * @see KMean
		 */
		DataStream<ArrayList<Tuple5<Integer, Integer, Float, String, Integer>>> streamAfterKMean = streamAfterAnalyze.keyBy(0, 1)
				.countWindow(countWindowSize, 1)
				.apply(new KMean(mapSensors_ClustersSeuils));

		/**
		 * Application de la map Markov
		 * @see Markov
		 */
		DataStream<Tuple5<Integer, Integer, Float, String, Double>> streamAfterMarkov = streamAfterKMean.flatMap(new Markov(mapSensors_ClustersSeuils));

		/**
		 * Application du filter FilterAnomalies
		 * @see FilterAnomalies
		 */
		DataStream<Tuple5<Integer, Integer, Float, String, Double>> streamAfterFilterAnomalies = streamAfterMarkov.filter(new FilterAnomalies(mapSensors_ClustersSeuils));
		
		//TODO 
		streamAfterFilterAnomalies.print();
		
		/**
		 * Application de la flatMap SortieTransformation
		 * @see SortieTransformation
		 */
		DataStream<String> streamAfterSortieTransformation = streamAfterFilterAnomalies.flatMap(new SortieTransformation());

		//TODO enlever ce print
		//streamAfterSortieTransformation.print();

		/**
		 * Création de la sortie du stream dans RabbitMQ
		 * connectionConfig : Configuration de la connection de RabbitMQ
		 * voila : le nom de la queue RabbitMQ à laquelle envoyer des messages
		 * true : 
		 * @see StreamExecutionEnvironment.addSink()
		 */
		streamAfterSortieTransformation.addSink(new RMQSink<String>(connectionConfig, // config for the RabbitMQ connection
				"voila", // name of the RabbitMQ queue to send messages to
				new SimpleStringSchema()));

		/**
		 * Exécution de l'environnement de streaming
		 * @see https://ci.apache.org/projects/flink/flink-docs-release-1.2/dev/api_concepts.html
		 */
		environnementStream.execute("DEBS 2017 ENSAI SOLUTION");
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////// USER FUNCTIONS ////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * @author ENSAI
	 * Traite les RDF en entrée pour ressortir des tuples contenant l'information pertinente.
	 * @return Tuple3<String, String, String>
	 * 											Tuple qui contient les trois élements du RDF : sujet, prédicat, objet
	 */
	public static final class LineSplitter1 implements FlatMapFunction<String, String> {

		@Override
		public void flatMap(String value,
				Collector<String> out) throws Exception {

			// normalize and split the line
			String[] lineOrLines = value.split("\n");

			for(int i = 0; i < lineOrLines.length; i++){
				out.collect(lineOrLines[i]);
			}
		}
	}

	public static final class LineSplitter2 implements FlatMapFunction<String, Tuple3<String, String, String>> {

		@Override
		public void flatMap(String value,
				Collector<Tuple3<String, String, String>> out) throws Exception {

			String[] tokens = StringUtils.split(value.toLowerCase()," ");
			out.collect(new Tuple3<String, String, String>(tokens[0], tokens[1], tokens[2]));

		}
	}

	public static final class LineSplitter3 implements FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

		@Override
		public void flatMap(Tuple3<String, String, String> value,
				Collector<Tuple3<String, String, String>> out) throws Exception {

			value.f0 = StringUtils.split(value.f0, "#")[1];

			out.collect(new Tuple3<String, String, String>(value.f0, value.f1, value.f2));

		}
	}

	public static final class LineSplitter4 implements FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

		@Override
		public void flatMap(Tuple3<String, String, String> value,
				Collector<Tuple3<String, String, String>> out) throws Exception {

			value.f1 = StringUtils.split(value.f1, "#")[1];

			out.collect(new Tuple3<String, String, String>(value.f0, value.f1, value.f2));

		}
	}

	public static final class LineSplitter5 implements FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

		@Override
		public void flatMap(Tuple3<String, String, String> value,
				Collector<Tuple3<String, String, String>> out) throws Exception {

			String[] tab = value.f2.split("\\^\\^");

			if (tab.length == 1) {
				value.f2 = StringUtils.split(tab[0],"#")[1];
			} else {
				value.f2 = tab[0];
			}
			out.collect(new Tuple3<String, String, String>(value.f0, value.f1, value.f2));

		}
	}
	
	/**
	 * @author ENSAI
	 * Filtre les tuples en entrée pour ressortir que les tuples dont leur probabilité de transition est inférieur à leur seuil metadata.
	 * @return Tuple5<Integer, Integer, Float, String, Double>
	 * 															Tuple qui présente une anomalie
	 */
	public static class FilterAnomalies extends RichFilterFunction<Tuple5<Integer, Integer, Float, String, Double>> {

		private Map<Integer, Tuple2<Integer, Double>> mapClustersSeuils;

		public FilterAnomalies(Map<Integer, Tuple2<Integer, Double>> mapClustersSeuils) {
			this.mapClustersSeuils = mapClustersSeuils;
		}

		@Override
		public boolean filter(Tuple5<Integer, Integer, Float, String, Double> input) throws Exception {
			//return (input.f4 < this.mapClustersSeuils.get(input.f1).f1);
			return true;
		}
	}

	/**
	 * @author ENSAI
	 * Applique l'algorithme KMean sur les tuples contenues dans la countWindow. L'id du cluster est ajouté au tuple par l'intermédiaire d'un nouveau field
	 * @return ArrayList<Tuple5<Integer, Integer, Float, String, Integer>
	 * 																		ArrayList de tuples qui constituent les clusters générés par KMean
	 */
	public static class KMean extends
	RichWindowFunction<Tuple4<Integer, Integer, Float, String>,  ArrayList<Tuple5<Integer, Integer, Float, String, Integer>>, Tuple, GlobalWindow> {

		private Map<Integer, Tuple2<Integer, Double>>  mapClustersSeuils;

		public KMean(Map<Integer, Tuple2<Integer, Double>> mapClustersSeuils) {
			this.mapClustersSeuils = mapClustersSeuils;
		}

		private Counter counterTempsKMean;

		/**
		 * Création de la métrique CounterKMean utilisée pour mesurer le temps d'exécution de la fonction.
		 */
		@Override
		public void open(Configuration config) {
			this.counterTempsKMean = getRuntimeContext()
					.getMetricGroup()
					.counter("CounterKMean");
		}

		@Override
		public void apply(Tuple key, GlobalWindow window, Iterable<Tuple4<Integer, Integer, Float, String>> input,
				Collector<ArrayList<Tuple5<Integer, Integer, Float, String, Integer>>> output) throws Exception {

			this.counterTempsKMean.dec(System.currentTimeMillis());

			int machine = 0;
			int capteur = 0;

			KMeans kmean = new KMeans();
			List<Point> listePoints = new ArrayList<Point>();
			List<Cluster> listeCluster = new ArrayList<Cluster>();
			List<Float> listCentroidCluster = new ArrayList<Float>();
			ArrayList<Tuple5<Integer, Integer, Float, String, Integer>> resultatKMean = new ArrayList<Tuple5<Integer, Integer, Float, String, Integer>>();
			int nbCluster = 0;

			/**
			 * Transformation des tuples en objets necessaire pour appliquer l'algorithme KMean
			 */
			for (Tuple4<Integer, Integer, Float, String> tuple : input) {
				if (nbCluster < mapClustersSeuils.get(tuple.f1).f0 && !listCentroidCluster.contains(tuple.f2)) {
					listCentroidCluster.add(tuple.f2);
					machine = tuple.f0;
					capteur = tuple.f1;
					Cluster cluster = new Cluster(nbCluster);
					cluster.setCentroid(new Point(tuple.f2));
					listeCluster.add(cluster);
					nbCluster++;
				}
				listePoints.add(new Point(tuple.f2, tuple.f3));

			}
			kmean.setPoints(listePoints);
			kmean.setNUM_POINTS(listePoints.size());

			kmean.setClusters(listeCluster);
			kmean.setNUM_CLUSTERS(listeCluster.size());

			kmean.calculate(maxIterationKMean);

			for (Point point : kmean.getPoints()) {
				resultatKMean.add(new Tuple5<Integer, Integer, Float, String, Integer>(machine, capteur, (float) point.getX(), point.getTimestamp(), point.getCluster()));
			}

			this.counterTempsKMean.inc(System.currentTimeMillis());

			output.collect(resultatKMean);
		}
	}

	 /**
     * @author ENSAI
     * Calcule les probabilités de transitions d'un cluster à un autre d'après le modèle des chaines de Markov
     * @return Tuple5<Integer, Integer, Float, String, Double>
     *                                                             Tuple d'interet
     */
	public static final class Markov extends RichFlatMapFunction<ArrayList<Tuple5<Integer, Integer, Float, String, Integer>>,
	Tuple5<Integer, Integer, Float, String, Double>>{

		private Map<Integer, Tuple2<Integer, Double>> mapClustersSeuils;

		public Markov(Map<Integer, Tuple2<Integer, Double>> mapClustersSeuils) {
			this.mapClustersSeuils = mapClustersSeuils;
		}

		        private Counter counter;
		
		        /**
		         * Création de la métrique CounterMarkov utilisée pour mesurer le temps d'exécution de la fonction.
		         */
		        @Override
		        public void open(Configuration config) {
		            this.counter = getRuntimeContext()
		                    .getMetricGroup()
		                    .counter("CounterMarkov");
		        }

		@Override
		public void flatMap(ArrayList<Tuple5<Integer, Integer, Float, String, Integer>> input,
				Collector<Tuple5<Integer, Integer, Float, String, Double>> out) throws Exception {

			this.counter.dec(System.currentTimeMillis());


			/** Number of clusters */
			int K =  this.mapClustersSeuils.get(input.get(0).f1).f0;
			/** Size of the window */
			int windowSize = input.size();
			if(windowSize==1){
				/** On sort le premier tuple avec comme proba : 1. */
				out.collect(new Tuple5(input.get(0).f0, input.get(0).f1, input.get(0).f2, input.get(0).f3, Double.valueOf(1)));
			}else{
				//System.out.println(windowSize);
				int trueN = Math.min(input.size()-1, N);
				Tuple5<Integer, Integer, Float, String, Double> output ;

				HashMap<Tuple2<Integer,Integer>,Integer> countTransitions = new HashMap<Tuple2<Integer,Integer>,Integer>();
				double[] countFrom = new double[K];

				Iterator<Tuple5<Integer, Integer, Float, String, Integer>> tupleIter = input.iterator();
				Tuple5<Integer, Integer, Float, String, Integer> tuple = tupleIter.next();

				Integer stateFrom = tuple.f4;

				while(tupleIter.hasNext()){
					countFrom[tuple.f4]++;
					tuple = tupleIter.next();

					Tuple2<Integer, Integer> transitionKey = new Tuple2<Integer,Integer>(stateFrom, tuple.f4);
					if(!countTransitions.containsKey(transitionKey)){
						countTransitions.put(transitionKey, 1);
					}else{
						countTransitions.put(transitionKey, countTransitions.get(transitionKey)+1);
					}        

					stateFrom = tuple.f4;
				}

				ArrayList<Integer> lastN = new ArrayList<Integer>(trueN+1);
				for(int j = 1 ; j<trueN+2; j++){
					lastN.add(input.get(windowSize-j).f4);
				}

				Double probability = Double.valueOf(1) ;
				for(int position = 0; position<trueN; position++){
					Tuple2<Integer,Integer> couple = new Tuple2<Integer,Integer>(lastN.get(position+1), lastN.get(position));
					probability = probability * countTransitions.get(couple)/countFrom[couple.f0];
					output = new Tuple5(tuple.f0, tuple.f1, tuple.f2, tuple.f3, probability);
					out.collect(output);
				}
			}


			this.counter.inc(System.currentTimeMillis());

		}

	}
	
	/**
	 * @author ENSAI
	 * Analyse des triplets RDF pour renvoyer les tuples pertinents 
	 * @return Tuple4<Integer, Integer, Float, String>
	 * 													tuple qui contient : numéro de la machine, numéro du capteur, valeur de l'observation et le timestamp
	 */
	public static final class InputAnalyze
	extends RichFlatMapFunction<Tuple3<String, String, String>, Tuple4<Integer, Integer, Float, String>> {

		private Map<Integer, Tuple2<Integer, Double>> mapClustersSeuils;

		private Counter counter;

		/**
		 * Création de la métrique CounterInputAnalyze utilisée pour mesurer le temps d'exécution de la fonction.
		 */
		@Override
		public void open(Configuration config) {
			this.counter = getRuntimeContext()
					.getMetricGroup()
					.counter("CounterInputAnalyze");
		}

		public InputAnalyze(Map<Integer, Tuple2<Integer, Double>> mapClustersSeuils) {
			this.mapClustersSeuils = mapClustersSeuils;
		}

		@Override
		public void flatMap(Tuple3<String, String, String> input,
				Collector<Tuple4<Integer, Integer, Float, String>> out) throws Exception {

			this.counter.dec(System.currentTimeMillis());

			switch (input.f1) {

			case "machine>":
				RMQtestKmeans.currentMachine = Integer.parseInt(input.f2.split("\\_|>")[1]);
				//System.out.println("Current machine number :  " + RMQtestKmeans.currentMachine);
				break;

			case "type>":
				if (input.f0.split("\\_")[0].equals("ObservationGroup")) {
					RMQtestKmeans.currentMachineType = input.f2.split(">")[0];
					//System.out.println("Current machine type : " + RMQtestKmeans.currentMachineType);
				}
				break;

			case "observationresulttime>":
				RMQtestKmeans.currentObsGroup = Integer.parseInt(input.f0.split("\\_|>")[1]);
				//System.out.println("Current Observation group : " + RMQtestKmeans.currentObsGroup);
				break;

			case "observedproperty>":

				// System.out.println("Numéro de capteur : " +
				// Integer.parseInt(value.f2.split("\\_|>")[1]));
				// System.out.println("Numéro d'observation : " +
				// Integer.parseInt(value.f0.split("\\_|>")[1]));
				
				input.f2 = StringUtils.split(input.f2, "_")[1];
				int sensorNb = Integer.parseInt(input.f2.split(">")[0]);
				//System.out.println("Current capteur : " + value.f2);
				
				int obsNb = Integer.parseInt(input.f0.split("\\_|>")[1]);

				// System.out.println(listSensors.contains(sensorNb));
				// System.out.println(listSensors);
				// System.out.println(sensorNb);
				
				//TODO Gérer le cas du sensor 104 
				if (mapClustersSeuils.containsKey(sensorNb) && sensorNb != 104) {
					RMQtestKmeans.mapObsSensors.put(obsNb, sensorNb);
					// System.out.println("Numéro d'observation enregisté");
				} else {
					// System.out.println("Ce capteur ne nous interesse pas
					// !!");
				}

				break;

			case "valueliteral>":

				String[] tab = input.f0.split("\\_|>");

				if (tab[0].split("\\_")[0].equals("timestamp")) {
					RMQtestKmeans.currentTimestamp = input.f2;
					//System.out.println("Current timestamp : " + RMQtestKmeans.currentTimestamp);
				}

				else if (tab[0].equals("value") && RMQtestKmeans.mapObsSensors.containsKey(Integer.parseInt(tab[1]))) {
					// System.out.println(" ================= > " + "Capteur : "
					// +
					// StreamRabbitMQ.mapObsSensors.get(Integer.parseInt(tab[1]))
					// + " | Valeur : "
					// + Float.parseFloat(value.f2.split("\"")[1]));

					out.collect(new Tuple4<Integer, Integer, Float, String>(RMQtestKmeans.currentMachine,
							RMQtestKmeans.mapObsSensors.get(Integer.parseInt(tab[1])),
							Float.parseFloat(input.f2.split("\"")[1]), RMQtestKmeans.currentTimestamp));

					RMQtestKmeans.mapObsSensors.remove(Integer.parseInt(tab[1]));

				}
				break;
			}
			
			this.counter.inc(System.currentTimeMillis());
		}
	}

	/**
	 * @author ENSAI
	 * Transformation des tuples d'entrée en sortie RDF
	 * @return String
	 * 					Sortie RDF des tuples concernés
	 */
	public static final class SortieTransformation
	extends RichFlatMapFunction<Tuple5<Integer, Integer, Float, String, Double>, String> {

		private Counter counter;

		/**
		 * Création de la métrique CounterSortieTransformation utilisée pour mesurer le temps d'exécution de la fonction.
		 */
		@Override
		public void open(Configuration config) {
			this.counter = getRuntimeContext()
					.getMetricGroup()
					.counter("CounterSortieTransformation");
		}

		@Override
		public void flatMap(Tuple5<Integer, Integer, Float, String, Double> avant, Collector<String> apres)
				throws Exception {

			this.counter.dec(System.currentTimeMillis());

			String numMachine = avant.f0.toString();
			String numSensor = avant.f1.toString();
			String valeur = avant.f2.toString();
			String timestamp = avant.f3.toString();
			String probTrans = avant.f4.toString();

			numeroAnomalie++;
			numeroTimestamp++;
			String numAno = Integer.toString(numeroAnomalie);
			String numTsp = Integer.toString(numeroTimestamp);

			String l1 = "<http://PFEensai/outputs/debs2017#Anomaly_" + numAno
					+ "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#Anomaly> .";
			String l2 = "<http://PFEensai/outputs/debs2017#Anomaly_" + numAno
					+ "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasProbabilityOfObservedAbnormalSequence> \""
					+ probTrans + "\"^^<http://www.w3.org/2001/XMLSchema#float> .";
			String l3 = "<http://PFEensai/outputs/debs2017#Anomaly_" + numAno
					+ "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#hasTimestamp> <http://project-hobbit.eu/resources/debs2017#Timestamp_"
					+ numTsp + "> .";
			String l4 = "<http://PFEensai/outputs/debs2017#Anomaly_" + numAno
					+ "> <http://www.agtinternational.com/ontologies/DEBSAnalyticResults#inAbnormalDimension> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#_"
					+ numSensor + "> .";
			String l5 = "<http://PFEensai/outputs/debs2017#Anomaly_" + numAno
					+ "> <http://www.agtinternational.com/ontologies/I4.0#machine> <http://www.agtinternational.com/ontologies/WeidmullerMetadata#MoldingMachine_"
					+ numMachine + "> .";
			String l6 = "<http://PFEensai/outputs/debs2017#Timestamp_" + numTsp
					+ "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.agtinternational.com/ontologies/IoTCore#Timestamp> .";
			String l7 = "<http://PFEensai/outputs/debs2017#Timestamp_" + numTsp
					+ "> <http://www.agtinternational.com/ontologies/IoTCore#valueLiteral> \"" + timestamp
					+ "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .";

			this.counter.inc(System.currentTimeMillis());

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
