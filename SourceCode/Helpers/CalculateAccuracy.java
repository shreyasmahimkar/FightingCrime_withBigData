import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import au.com.bytecode.opencsv.CSVReader;


public class CalculateAccuracy {
	
	public static void accuracy() throws ParseException, IOException{
		CSVReader reader = new CSVReader(new FileReader("C:/Users/NADEEM/Desktop/Crime data/chicagoanov.csv"));
		HashMap<String,HashSet<String>> map = new HashMap<String, HashSet<String>>();
		String [] nextLine;
		try {
			
			//Reading file containing actual occurrence of crime
			while ((nextLine = reader.readNext()) != null) {
				Date dt = new Date(nextLine[1]);     //Chicago
				//Date dt = new Date(nextLine[6]);   //Boston
				int hr = dt.getHours();
				int month = dt.getMonth()+1;
				
				String zip = nextLine[5];       //Chicago  
				//String zip = nextLine[20];    //Boston
				
				String street = nextLine[0].toLowerCase().trim();     //Chicago
				//String street = nextLine[17].toLowerCase().trim();   //Boston
				
				String key = zip+","+hr+","+month;
				
				HashSet<String> streetList = new HashSet<String>(); 
				if(map.containsKey(key)){
					streetList = map.get(key);
					streetList.add(street);
					map.put(key,streetList);
				}else{
					streetList.add(street);
					map.put(key,streetList);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		File folder = new File("C:/Users/NADEEM/Desktop/Crime data/4D 1 nearest chicago/");
		File[] listOfFiles = folder.listFiles();
		double totalAccuracy = 0;
		double totalPredicted = 0;
		File file =new File("4D_1_nearest_chicago_Accuracy.txt");
		 
		if(!file.exists()){
			file.createNewFile();
		}
		
		FileWriter fileWritter = new FileWriter(file.getName(),true);
        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
		for (File f : listOfFiles) {
			System.out.println(f);
			BufferedReader br = new BufferedReader(new FileReader(f));
			String line;
			
		
			try {
				while ((line = br.readLine()) != null) {
				   String zip = line.split("\t")[0].split(",")[0];
				   int hr = Integer.parseInt(line.split("\t")[0].split(",")[1]);
				   
				   String strDate = line.split("\t")[0].split(",")[2];
				   Date dt = new Date(strDate);
				   int month = dt.getMonth()+1;
				
				   String key = zip+","+hr+","+month;
				   String[]  predictedStreet = line.split("\t")[1].split(",");
				  
				   double foundCount=0;
				 
				   HashSet<String> actualStreets = null ;
				   
				   HashSet<String> iteratedStreet = new HashSet<String>();
				   
				   for (String strPredicted : predictedStreet) {
						if(!iteratedStreet.contains(strPredicted)){
						   iteratedStreet.add(strPredicted);
						  
						   strPredicted = strPredicted.split(":")[0];
						
						   if(map.containsKey(key)){
							   actualStreets = new HashSet<String>();
							   actualStreets  = map.get(key);
				
							   if(actualStreets.size()!=0){
								for (String strActual : actualStreets) {
								
									//if(strActual.toLowerCase().contains(strPredicted.toLowerCase())){
									if(strPredicted.toLowerCase().contains(strActual.toLowerCase())){
										foundCount++;
			
									}
								}
							  }  
						   }
					   }
				   }
				   
				   iteratedStreet.clear(); 
				    
				   if(map.containsKey(key)){
				   	  StringBuilder sb = new StringBuilder();
				   	  for (String string : actualStreets) {
						sb.append(string+",");
					  }
				   	  totalPredicted++;
				   	  totalAccuracy = totalAccuracy + (foundCount/actualStreets.size())*100;
					  bufferWritter.write(line+"\t"+actualStreets.size()+"\t"+foundCount+"\t"+(foundCount/actualStreets.size())*100);
					  bufferWritter.newLine();
					  bufferWritter.write(sb.toString());
					  bufferWritter.newLine();
				   }
				}
				
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Accuracy  :"+totalAccuracy/totalPredicted);
		System.out.println(totalAccuracy);
		System.out.println(totalPredicted);
		bufferWritter.write("Accuracy  :"+totalAccuracy/totalPredicted+"%");
		bufferWritter.close();
		
	}
	
	public static void main(String args[]) throws ParseException, IOException{
		accuracy();
	}
}

