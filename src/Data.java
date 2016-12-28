import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;




public class Data {
	
	
	public static List<Table> getTables(DatabaseMetaData database, String schemaName) throws SQLException{
		
		List<Table> tables=new ArrayList<Table>();
		ResultSet result=database.getTables(schemaName, null, null, null);
		while (result.next()) {
			String tableName=result.getString("TABLE_NAME");
			Table table = new Table(schemaName,tableName,null,null);
			table.getColumns(database, schemaName, tableName);
			table.getPrimaryKeys(database, schemaName, tableName);
			table.hasUniqueConstraint(database, schemaName, tableName);
			tables.add(table);
		}
		return tables;
	}
	
	
	public static List<Table> getPK(DatabaseMetaData database, Statement stmt, String schemaName){
		List<Table> schema=new ArrayList<Table>();
		Connection connection=null;
		try {
			connection=getSchemaConnection(schemaName);
			schema=getTables(database,schemaName);
			Iterator<Table> it=schema.iterator();
			
			while(it.hasNext()){
				Table table=it.next();
				
				Iterator<Column> ic=table.getColumnList().iterator();
				while(ic.hasNext()){
					Column column=ic.next();
					column.isUnique(connection, table.getName());
					column.calculateLD(table.getName());
					column.checkEnds();
					column.calculateReps(schema);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return schema;
		
	}
	public static Connection getConnection() throws Exception{
		
		Connection connection = null;
		
		try{
			
			String connectionURL = "jdbc:mysql://relational.fit.cvut.cz";
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(connectionURL,"guest","relational");		
			
		} catch (Exception e){
			e.printStackTrace();

		}
		return connection;
	}
	public static Connection getSchemaConnection(String schemaName){
		Connection connection=null;
		try{
			
			String connectionURL = "jdbc:mysql://relational.fit.cvut.cz";
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection = DriverManager.getConnection(connectionURL+"/"+schemaName,"guest","relational");
		
		} catch (Exception e){
		e.printStackTrace();
		}
		return connection;
	}

	
	public static void main(String[] args) {
		
		List<Table> schema=new ArrayList<Table>();
		try {
			
			Connection connection = getConnection();
			DatabaseMetaData database = connection.getMetaData();
			Statement stmt = connection.createStatement();

			PrintWriter writer = new PrintWriter("data.ods", "UTF-8");

			ResultSet result = stmt.executeQuery("select distinct TABLE_SCHEMA from information_schema.columns"
					+ " where TABLE_SCHEMA not in ('information_schema', 'predictor_factory', 'mysql', 'meta', 'Phishing', 'fairytale')"
					+ " and TABLE_SCHEMA not like 'arnaud_%' and TABLE_SCHEMA not like 'ctu_%'");
			
			while (result.next()) {
				
				
				String schemaName=result.getString(1);
				schema=getPK(database,stmt,schemaName);
				
				Iterator<Table> it=schema.iterator();
				while(it.hasNext()){
				
					writer.print(it.next().toString());
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

}
