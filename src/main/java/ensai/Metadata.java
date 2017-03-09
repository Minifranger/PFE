package ensai;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;


public class Metadata {
	
	static String cheminMeta = "/home/quentin/PFE-master/ressources/sample_metadata_1machine.nt";
	static String chemin = "/home/quentin/PFE-master/ressources/metadata";

	static String[] lineSplit = new String[3];
	//Tuple type de la machine, numero de la machine, capteurs à considérer, nbre de clusters pour ces capteurs, seuils pour ces capteurs
	//public static Tuple5<String, Integer, List<Integer>, List<Integer>, List<Float> > metaUneMachine = new Tuple5<String, Integer, List<Integer>, List<Integer>, List<Float> >(); 

	//Hasmap avec en clef MoldingMachine_0, et en valeur les trois listes des capteurs, nbre clusters et seuils
	//public static HashMap<String, Tuple3<List<Integer>, List<Integer>, List<Float>>> meta = new HashMap<String, Tuple3<List<Integer>, List<Integer>, List<Float>>>();

	public static HashMap<Integer, Tuple2<Integer, Float>> moldingMeta = new HashMap<Integer, Tuple2<Integer, Float>>();
	static String numMachineCapteur = "";
	static String numCapteur = "";
	static String nbClusters = ""; 
	static String seuil  = "";
	
	public static HashMap<Integer, Tuple2<Integer, Float>> injectionMeta = new HashMap<Integer, Tuple2<Integer, Float>>();
	static String numMachineCapteurI = "";
	static String numCapteurI = "";
	static String nbClustersI = ""; 
	static String seuilI  = "";
	
	static String type = "";
	
	
	public static void readData() throws IOException {

		//Lecture du fichier
		File f = new File(cheminMeta);

		BufferedReader br = new BufferedReader(new FileReader(f));
		String line;
		String[] doubleEspace = new String[2];

		//Récupération des données utiles dans chaque ligne
		while ((line = br.readLine()) != null) {

			doubleEspace = line.split("  ");

			if(doubleEspace.length == 1){
				lineSplit = line.split(" ");
			} if(doubleEspace.length == 2){
				lineSplit[0] = doubleEspace[0];
				lineSplit[1] = doubleEspace[0].split(" ")[0];
				lineSplit[2] = doubleEspace[1].split(" ")[1];
			} else {
				//System.out.println("?");
			}

			lineSplit[0] = lineSplit[0].split("#")[1];
			lineSplit[1] = lineSplit[1].split("#")[1];

			String[] tab = lineSplit[2].split("\\^\\^");
			if (tab.length == 1) {
				lineSplit[2] = tab[0].split("#")[1];
			} if (tab.length == 2) {
				lineSplit[2] = tab[0];
			} else {
				//System.out.println("?");
			}

		//	System.out.println(lineSplit[0] + " " + lineSplit[1] + " " + lineSplit[2]);

			//Tri
			switch (lineSplit[1]) {

			//le numero de capteur
			case "hasProperty>":
				numMachineCapteur = lineSplit[2].split(">")[0];
				numCapteur = numMachineCapteur.split("_")[2];
				//}
				break;

				//le nombre de clusters
			case "hasNumberOfClusters>":
				if(lineSplit[0].contains(numMachineCapteur)){
					nbClusters = lineSplit[2].split("\"")[1];
				}
				break;

				//le seuil
			case "valueLiteral>":
				if(lineSplit[0].contains(numMachineCapteur)){
					seuil = lineSplit[2].split("\"")[1];
					break;
				}
			}
			
			if(lineSplit[2].contains("MoldingMachine")){
				type = "MoldingMachine";
			} if(lineSplit[2].contains("InjectionMachine")){
				type = "InjectionMachine";
			}
			
			//remplissage de la map molding
			if(type == "MoldingMachine" && numMachineCapteur!="" && numCapteur!="" && nbClusters!="" && seuil!=""){
				Tuple2<Integer, Float> clustersEtseuil = new Tuple2<Integer, Float>();
				clustersEtseuil.f0 = Integer.parseInt(nbClusters);
				clustersEtseuil.f1 = Float.parseFloat(seuil);
				moldingMeta.put(Integer.parseInt(numCapteur), clustersEtseuil);
			}
			//remplissage de la map injection
			if(type == "InjectionMachine" && numMachineCapteurI!="" && numCapteurI!="" && nbClustersI!="" && seuilI!=""){
				Tuple2<Integer, Float> clustersEtseuilI = new Tuple2<Integer, Float>();
				clustersEtseuilI.f0 = Integer.parseInt(nbClustersI);
				clustersEtseuilI.f1 = Float.parseFloat(seuilI);
				injectionMeta.put(Integer.parseInt(numCapteurI), clustersEtseuilI);
			}

			//			System.out.println(numMachineCapteur);
			//			System.out.println(numCapteur);
			//			System.out.println(nbClusters);
			//			System.out.println(seuil);

		}
		System.out.println(moldingMeta);

		br.close();
		
		saveMap(moldingMeta);


	}

	
	public static void saveMap (HashMap<Integer, Tuple2<Integer, Float>> map) throws IOException{
		
		Properties prop = new Properties();
		
		File f = new File (chemin);
		
		if (!f.exists()){
			f.createNewFile();
		}
		 FileOutputStream out = new FileOutputStream(chemin);
		
		 Set cles = map.keySet();
		 Iterator<Integer> it = cles.iterator();
		 while (it.hasNext()) {
		 int cle = it.next();
		 String value = map.get(cle).f0 + "|" + map.get(cle).f1;
	
		 prop.setProperty(Integer.toString(cle), value);
		 }
		 
		 prop.store(out, "-------Metadata-------");
		 out.close();
	}
	
	public static HashMap<Integer, Tuple2<Integer, Double>> load() throws IOException{
		 HashMap<Integer, Tuple2<Integer, Double>> res = new  HashMap<Integer, Tuple2<Integer, Double>>();
		
			Properties prop = new Properties();

			FileInputStream in = new FileInputStream(chemin);
			prop.load(in);
			in.close();

			for (Object c : prop.keySet()) {
				
				//System.out.println(prop.getProperty((String) c).split("\\|")[0]);
				int cluster = Integer.parseInt(prop.getProperty((String) c).split("\\|")[0]);
				Double seuil = Double.parseDouble(prop.getProperty((String) c).split("\\|")[1]);
				
				res.put(Integer.parseInt((String) c), new Tuple2<Integer, Double>( cluster, seuil)) ;
			}
			
			
		return res;
		
	}
}


