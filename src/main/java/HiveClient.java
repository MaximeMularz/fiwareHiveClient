import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveClient {
	// JDBC driver required for Hive connections
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static Connection con;

	private static Connection getConnection(String ip, String port,
			String user, String password) {
		try {
			// dynamically load the Hive JDBC driver
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			System.out.println("Class.forName(driverName) :" + e.getMessage());
			return null;
		} // try catch

		try {
			// return a connection based on the Hive JDBC driver
			return DriverManager.getConnection("jdbc:hive://" + ip + ":" + port
					+ "/default?user=" + user + "&password=" + password);
		} catch (SQLException e) {
			System.out.println("DriverManager : " + e.getMessage());
			return null;
		} // try catch
	} // getConnection

	private static void doQuery() {
		try {
			// from here on, everything is SQL!
			Statement stmt = con.createStatement();
			
			if(stmt != null){
				System.out.println("It works");
			}
			
			ResultSet res = stmt
					.executeQuery("select *"
							+ "from 4planet");
			
			// iterate on the result
				  // iterate on the result
	            while (res.next()) {
	                String s = "";

	                for (int i = 1; i < res.getMetaData().getColumnCount(); i++) {
	                    s += res.getString(i) + ",";
	                } // for
	              
	                s += res.getString(res.getMetaData().getColumnCount());
	                System.out.println(s);
	            } 

			// close everything
			res.close();
			stmt.close();
			con.close();
		} catch (SQLException ex) {
			System.exit(0);
		} // try catch
	} // doQuery

	public static void main(String[] args) {
		// get a connection to the Hive server running on the specified IP
		// address, listening on 10000/TCP port
		// authenticate using my credentials
		con = getConnection("130.206.80.46", "10000", "user.name",
				"password");

		if (null != con)
			doQuery();
		// do a query, querying the Hive server will automatically imply the
		// execution of one or more MapReduce jobs

	} // main
} // HiveClient