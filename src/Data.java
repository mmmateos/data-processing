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
	
	public static List<Column> getColumns(DatabaseMetaData database, String schemaName, String tableName) throws SQLException{
		
		List<Column> columns=new ArrayList<Column>();
		ResultSet result=database.getColumns(schemaName,null,tableName,null);
		
		while(result.next()){
			Column column = new Column(result.getInt("DATA_TYPE"),
					result.getString("TYPE_NAME"),result.getString("COLUMN_NAME"));
			column.setNullable(result.getBoolean("NULLABLE"));
			columns.add(column);
		}
		return columns;
	}
	public static List<Table> getTables(DatabaseMetaData database, String schemaName) throws SQLException{
		
		List<Table> tables=new ArrayList<Table>();
		ResultSet result=database.getTables(schemaName, null, null, null);
		while (result.next()) {
			String tableName=result.getString("TABLE_NAME");
			Table table = new Table(schemaName, tableName,getColumns(database,schemaName,tableName),null);
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
					column.isUniqueConstraint(database, schemaName, table.getName());
					column.calculateLD(table.getName());
					column.checkEnds();
					column.calculateReps(schema);
					column.isPrimaryKey(database, schemaName, table.getName());
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
		//List<String> catalogs=new ArrayList<String>();
		List<Table> schema=new ArrayList<Table>();
		try {
			
			Connection connection = getConnection();
			DatabaseMetaData database = connection.getMetaData();
			Statement stmt = connection.createStatement();

			

			ResultSet result = database.getCatalogs();
			
			while (result.next()) {
				String schemaName=result.getString(1);
				schema=getPK(database,stmt,schemaName);
				
				Iterator<Table> it=schema.iterator();
				while(it.hasNext()){
				
					System.out.println(it.next().toString());
				}
			}
						
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
