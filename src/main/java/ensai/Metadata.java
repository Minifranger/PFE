package ensai;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple5;


public class Metadata {

	static String[] lineSplit = new String[3];
	//Tuple type de la machine, numero de la machine, capteurs à considérer, nbre de clusters pour ces capteurs, seuils pour ces capteurs
	public static Tuple5<String, Integer, List<Integer>, List<Integer>, List<Float> > metaUneMachine = new Tuple5<String, Integer, List<Integer>, List<Integer>, List<Float> >(); 

	public static void main(String[] args) throws IOException {


		//Lecture du fichier
		File f = new File("./ressources/sample_metadata_1machine.nt");

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

			//System.out.println(lineSplit[0] + " " + lineSplit[1] + " " + lineSplit[2]);

			//Tri
			String MachineModel = "";
			String numMachineCapteur = "";
			switch (lineSplit[1]) {
			
			//le MachineModel suivi
			case "hasModel>":
				MachineModel = lineSplit[2].split(">")[0];
				metaUneMachine.f0 = MachineModel ; 
 				metaUneMachine.f1 = Integer.parseInt(lineSplit[0].split("_")[1].split(">")[0]);
				break;
				
			//le numero de capteur
			case "hasProperty>":
				if(lineSplit[0].split(">")[0] == MachineModel){
					numMachineCapteur = lineSplit[2].split(">")[0];
					metaUneMachine.f2.add(Integer.parseInt(lineSplit[2].split("_")[2].split(">")[0]));
				}
				break;
			
			//le nombre de clusters
			case "hasNumberOfClusters>":
				if(lineSplit[0].split(">")[0] == numMachineCapteur){
					metaUneMachine.f3.add(Integer.parseInt(lineSplit[2].split("\"")[2].split("\"")[0]));
				}
				break;
			
			//le seuil
			case "valueLiteral>":
				if(lineSplit[0].split(">")[0] == numMachineCapteur){
					metaUneMachine.f4.add(Float.parseFloat(lineSplit[2].split("\"")[2].split("\"")[0]));
				}
				break;
			
			}
			
			System.out.println(metaUneMachine.f0 + " " + metaUneMachine.f1 + " " + metaUneMachine.f2 + " " + metaUneMachine.f3 + " " + metaUneMachine.f4);
			
		}
		br.close();


	}

}
