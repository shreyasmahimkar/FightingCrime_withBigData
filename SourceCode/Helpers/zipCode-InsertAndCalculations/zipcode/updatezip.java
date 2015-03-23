package zipcode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;

import parsewebxml.XMLParserSAX;

public class updatezip {
	private static Connection conn = null;
	private static PreparedStatement preparedStatement = null;
	static int count = 0;

	public static void getJSONByGoogle(String ip, int port) throws IOException,
			ClassNotFoundException, SQLException, InterruptedException {
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = "crimedb";
		String userName = "root";
		String password = "";
		System.out.println(ip+":"+port);
		// TODO Auto-generated method stub
		// this will load the MySQL driver, each DB has its own driver
		Class.forName("com.mysql.jdbc.Driver");
		// setup the connection with the DB.
		conn = DriverManager.getConnection(url + dbName, userName, password);
		System.out.println("Connected to the database");
		double lati;
		double longi;
		String query = "SELECT distinct clat,clong FROM `crime` where city = 'Chicago' and zipcode is null";

		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(query);
		int maxcalls = 2500;
		int countofcalls = 0;
		while (rs.next() && countofcalls!=maxcalls) {
			lati = rs.getDouble("clat");
			longi = rs.getDouble("clong");
			Proxy proxy=new Proxy(Proxy.Type.HTTP, new InetSocketAddress(ip, port));
			//String url1 = "https://maps.googleapis.com/maps/api/geocode/xml?latlng="+ lati + "," + longi;
			String url1 = "http://nominatim.openstreetmap.org/reverse?format=xml&lat="+lati+"&lon="+longi+"&zoom=18&addressdetails=1";
			System.out.println(url1);
			URL url2 = new URL(url1);
			URLConnection connurl = url2.openConnection(proxy);
			System.out.println(connurl);
			ByteArrayOutputStream output = new ByteArrayOutputStream(1024);
			try{
				countofcalls+=1;
				IOUtils.copy(connurl.getInputStream(), output);
			}catch (Exception e){
				e.printStackTrace();
				return;
				
			}
			//System.out.println(output.toString());
			String zip = XMLParserSAX.ParseXML(output.toString());
			count+=1;
			System.out.println(zip +" : "+count);
			
			//Thread.sleep(200);
			query = "update crime set zipcode = ? where city = 'Chicago' and zipcode is null and clat ="+lati+"  and clong = "+longi;
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, zip);

			// execute the java preparedstatement
			preparedStatement.executeUpdate();
		}

		conn.close();
		System.out.println("----------------------All zips done------------------------");
		System.exit(0);
	}

	public static void main(String args[]) throws IOException,
			ClassNotFoundException, SQLException, InterruptedException {
		HashMap<String, String> h1;
		while (true) {
			h1 = Extract_ip_port.get_ip_port();
			for (Entry<String, String> e1 : h1.entrySet()) {
				getJSONByGoogle(e1.getKey(), Integer.parseInt(e1.getValue()));
			}
		}
	}
}
