package zipcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import au.com.bytecode.opencsv.CSVParser;

public class UploaddatatoRDB {
	private static Connection conn = null;
	private static PreparedStatement preparedStatement = null;
	static String block;// 17
	static String crimedate;// 6
	static String type;// 2
	static String city;
	static String state;
	static String zipcode;
	static double lat;// 19
	static double longi;// 19
	static double clat = 0;
	static double clong = 0;
	static CSVParser csvp = new CSVParser(',');
	static String[] tokens;

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {
		String url = "jdbc:mysql://localhost:3306/";
		String dbName = "crimedb";
		String userName = "root";
		String password = "";

		// TODO Auto-generated method stub
		// this will load the MySQL driver, each DB has its own driver
		Class.forName("com.mysql.jdbc.Driver");
		// setup the connection with the DB.
		conn = DriverManager.getConnection(url + dbName, userName, password);
		System.out.println("Connected to the database");
		// our SQL SELECT query.
		// if you only need a few columns, specify them by name instead of using
		// "*"
		String query = "SELECT * FROM crime";
		// create the java statement
		Statement st = conn.createStatement();

		// execute the query, and get a java resultset
		ResultSet rs = st.executeQuery(query);

		while (rs.next()) {
			block = rs.getString("block");
			crimedate = rs.getString("date");
			type = rs.getString("type");
			city = rs.getString("city");
			state = rs.getString("state");
			zipcode = rs.getString("zipcode");
			lat = rs.getDouble("lat");
			longi = rs.getDouble("longi");
			clat = rs.getDouble("clat");
			clong = rs.getDouble("clong");

		}
		query = " insert into crime (block, date, type, city, state,zipcode, lat,longi,clat,clong)"
				+ " values (?, ?, ?, ?, ?,?, ?, ?, ?, ?)";
		// create the mysql insert preparedstatement
		preparedStatement = conn.prepareStatement(query);
		Chicagodata();
		// Bostondata();

		conn.close();

	}

	public static void Chicagodata() throws NumberFormatException, IOException,
			SQLException {
		BufferedReader br = new BufferedReader(new FileReader(
				"C:\\Users\\mahimsh1\\workspace\\MR\\zipcode\\chi.csv"));
		String line = null;
		int count = 0;

		while ((line = br.readLine()) != null) {
			// process the line.
			if (count > 0) {
				tokens = csvp.parseLine(line);
				block = tokens[3];
				crimedate = tokens[2];
				type = tokens[5];
				city = "Chicago";
				state = "IL";
				zipcode = null;

				try {
					lat = Double.parseDouble(tokens[19]);
					longi = Double.parseDouble(tokens[20]);
				} catch (NumberFormatException ne) {
					continue;
				}
				clat = Math.round(lat * 10000);
				clong = Math.round(longi * 10000);

				preparedStatement.setString(1, block);
				preparedStatement.setString(2, crimedate);
				preparedStatement.setString(3, type);
				preparedStatement.setString(4, city);
				preparedStatement.setString(5, state);
				preparedStatement.setString(6, zipcode);
				preparedStatement.setDouble(7, lat);
				preparedStatement.setDouble(8, longi);
				preparedStatement.setDouble(9, clat / 10000);
				preparedStatement.setDouble(10, clong / 10000);

				// execute the preparedstatement
				preparedStatement.execute();
			}
			count++;
		}

		br.close();
	}

	public static void Bostondata() throws NumberFormatException, IOException,
			SQLException {
		BufferedReader br = new BufferedReader(new FileReader(
				"C:\\Users\\mahimsh1\\workspace\\MR\\zipcode\\bos.csv"));
		String line = null;
		int count = 0;

		while ((line = br.readLine()) != null) {
			// process the line.
			if (count > 0) {

				tokens = csvp.parseLine(line);
				block = tokens[17];
				crimedate = tokens[6];
				type = tokens[2];
				city = "Boston";
				state = "MA";
				zipcode = null;
				String[] ll = csvp.parseLine(tokens[19]);
				lat = Double.parseDouble(ll[0].substring(1));
				longi = Double.parseDouble(ll[1].substring(0,
						ll[1].length() - 1));
				clat = Math.round(lat * 10000);
				clong = Math.round(longi * 10000);

				preparedStatement.setString(1, block);
				preparedStatement.setString(2, crimedate);
				preparedStatement.setString(3, type);
				preparedStatement.setString(4, city);
				preparedStatement.setString(5, state);
				preparedStatement.setString(6, zipcode);
				preparedStatement.setDouble(7, lat);
				preparedStatement.setDouble(8, longi);
				preparedStatement.setDouble(9, clat / 10000);
				preparedStatement.setDouble(10, clong / 10000);

				// execute the preparedstatement
				preparedStatement.execute();
			}
			count++;
		}

		br.close();
	}

}
