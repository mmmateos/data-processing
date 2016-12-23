import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Table {

	String schemaName;										//Schema name
	String name;                                            // Table name
    List<Column> columnList = new ArrayList<>();   			// All columns in the table (Could be a list, if you dislike the Map)
    List<Column> primaryKey = new ArrayList<>();            // The most likely PK (PK can be a composite -> List)
	
    public Table(String schemaName, String name, List<Column> columnList, List<Column> primaryKey) {
		this.schemaName = schemaName;
    	this.name = name;
		this.columnList = columnList;
		this.primaryKey = primaryKey;
	}

    
	public String getSchemaName() {
		return schemaName;
	}


	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Column> getColumnList() {
		return columnList;
	}

	public void setColumnList(List<Column> columnList) {
		this.columnList = columnList;
	}

	public List<Column> getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(List<Column> primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	public void getColumns(DatabaseMetaData database, String schemaName, String tableName) throws SQLException{
		
		List<Column> columns=new ArrayList<Column>();
		ResultSet result=database.getColumns(schemaName,null,tableName,null);
		
		while(result.next()){
			Column column = new Column(result.getInt("DATA_TYPE"),
					result.getString("TYPE_NAME"),result.getString("COLUMN_NAME"));
			column.setNullable(result.getBoolean("NULLABLE"));
			columns.add(column);
		}
		this.columnList = columns;
	}
	
	public void getPrimaryKeys(DatabaseMetaData database, String schemaName, String tableName) throws SQLException{
		List<Column> primaryKeys=new ArrayList<Column>();
		ResultSet result = database.getPrimaryKeys(schemaName , schemaName, tableName);
		while(result.next()){
			Iterator<Column> it=this.columnList.iterator();
			while(it.hasNext()){
				Column col=it.next();
				if(col.name.equals(result.getString(4))){
					primaryKeys.add(col);
					col.setPrimaryKey(true);
				}
			}
		}
		this.primaryKey = primaryKeys;
	}
	
	public String toString(){
		String table="";
		Iterator<Column> it=this.columnList.iterator();
		while(it.hasNext()){
			table+=this.schemaName+"\t"+this.name+"\t"+it.next().toString()+"\n";
		}
		return table;
	}
    
}
