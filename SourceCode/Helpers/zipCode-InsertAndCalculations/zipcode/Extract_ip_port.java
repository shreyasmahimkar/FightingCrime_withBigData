package zipcode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import au.com.bytecode.opencsv.CSVParser;

public class Extract_ip_port {
	static CSVParser csvp = new CSVParser(' ');
	static CSVParser csvpinn = new CSVParser(':');
	
	static HashMap<String, String> h = new HashMap<String, String>();
	public static HashMap<String, String> get_ip_port() throws IOException{
		System.out.println("Construct ip and port");
		String fullpage = "";
		String url1 = "http://www.ip-adress.com/proxy_list/?k=time&d=desc";
		System.out.println(url1);
		URL url2 = new URL(url1);
		HttpURLConnection httpcon = (HttpURLConnection) url2.openConnection();
		httpcon.setRequestProperty("User-Agent", "Mozilla/5.0");
		BufferedReader br = new BufferedReader(new InputStreamReader((InputStream) httpcon.getContent()));
		String strTemp = "";
		while (null != (strTemp = br.readLine())) {
			fullpage = fullpage + strTemp;
		}
		//System.out.println(fullpage);
		Document doc = Jsoup.parse(fullpage);
		String[] tokens = csvp.parseLine(doc.text());
		int i =0;
		int j =0;
		while (true){
			if (tokens[i].equalsIgnoreCase("Elite")){
				String[] t = csvpinn.parseLine(tokens[i-1]);
				h.put(t[0],t[1]);
				j+=1;
			}
			if (j==15){
				return h;
			}
			i+=1;
		}
	}
	
	public static void main(String args[]) throws IOException {
		// TODO Auto-generated method stub
		HashMap<String, String> h1;
		h1 = get_ip_port();
	}
	
}
