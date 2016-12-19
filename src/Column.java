import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Column {
	
	int dataType;            // Data type as defined by JDBC
    String dataTypeName;     // Data type as defined by database
    boolean isNullable;      // From JDBC
    boolean isUnique;        // From JDBC
    boolean isUniqueConstraint;
    String name;             // Column name
    Map<String,Boolean> endsWith;  	//Map of booleans of ends
    int levenshteinDistance;		//Levenshtein Distance
    int repetitions;				//Number of repetitions on the schema
    double primaryKeyProbability;    // Estimated probability that this column alone is a PK 
    boolean isPrimaryKey;
	
    
    public Column(int dataType, String dataTypeName, String name) {

    	this.dataType = dataType;
		this.dataTypeName = dataTypeName;
		this.name = name;
	}


	public int getDataType() {
		return dataType;
	}


	public void setDataType(int dataType) {
		this.dataType = dataType;
	}


	public String getDataTypeName() {
		return dataTypeName;
	}


	public void setDataTypeName(String dataTypeName) {
		this.dataTypeName = dataTypeName;
	}


	public boolean isNullable() {
		return isNullable;
	}


	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
	}


	public boolean isUnique() {
		return isUnique;
	}


	public void setUnique(boolean isUnique) {
		this.isUnique = isUnique;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public double getPrimaryKeyProbability() {
		return primaryKeyProbability;
	}


	public void setPrimaryKeyProbability(double primaryKeyProbability) {
		this.primaryKeyProbability = primaryKeyProbability;
	}


	public Map<String, Boolean> getEndsWith() {
		return endsWith;
	}


	public void setEndsWith(Map<String, Boolean> endsWith) {
		this.endsWith = endsWith;
	}


	public int getLevenshteinDistance() {
		return levenshteinDistance;
	}


	public void setLevenshteinDistance(int levenshteinDistance) {
		this.levenshteinDistance = levenshteinDistance;
	}


	public int getRepetitions() {
		return repetitions;
	}


	public void setRepetitions(int repetitions) {
		this.repetitions = repetitions;
	}
    
	public void calculateLD(String a) {
        String b=this.name;
        
		a = a.toLowerCase();
        b = b.toLowerCase();
                int [] costs = new int [b.length() + 1];
        for (int j = 0; j < costs.length; j++)
            costs[j] = j;
        for (int i = 1; i <= a.length(); i++) {
            costs[0] = i;
            int nw = i - 1;
            for (int j = 1; j <= b.length(); j++) {
                int cj = Math.min(1 + Math.min(costs[j], costs[j - 1]), a.charAt(i - 1) == b.charAt(j - 1) ? nw : nw + 1);
                nw = costs[j];
                costs[j] = cj;
            }
        }
        this.levenshteinDistance = costs[b.length()];
    }
	
	public void checkEnds(){
		String[] ends={"aux","code","id","key","name","nbr","no","pk","sk","type"};
		Map<String,Boolean> endWith= new HashMap<String,Boolean>();
		
		for(int i=0;i<ends.length;i++){
			if(this.name.endsWith(ends[i]))
				endWith.put(ends[i], true);
			else
				endWith.put(ends[i], false);
		}
		this.endsWith = endWith;
	}
	
	public void calculateReps(List<Table> schema){
		int cont=0;
		Iterator<Table> it=schema.iterator();
		while(it.hasNext()){
			Iterator<Column> ic=it.next().getColumnList().iterator();
			while(ic.hasNext()){
				if(this.name.equals(ic.next().getName()))
					cont++;
			}
		}
		this.repetitions = cont;
	}
	public void isUnique(Connection conn, String tableName) throws Exception{
		
		ResultSet rs = null;
		String query="select count("+this.name+") - count(distinct "+this.name+") from "+tableName;
		
			try{
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
		
			if(rs.next()){
				this.isUnique = rs.getInt(1)==0 ? true:false;
			}
			
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	public void isUniqueConstraint(DatabaseMetaData database, String schemaName, String tableName) throws SQLException{
		ResultSet result=database.getIndexInfo(schemaName, schemaName, tableName, true, true);
		while(result.next()){
			this.isUniqueConstraint = result.getBoolean("NON_UNIQUE");
		}
	}
	public void isPrimaryKey(DatabaseMetaData database, String schemaName, String tableName) throws SQLException{
		ResultSet result = database.getPrimaryKeys(schemaName , schemaName, tableName);
		while(result.next()){
			this.isPrimaryKey = result.getString(6).equals("PRIMARY") ? true:false;
		}
	}
	
	public String toString(){
		return this.name+" "+this.dataTypeName+" "+this.isUnique+" "+this.isUniqueConstraint+" "+
				this.isNullable+" "+this.levenshteinDistance+" "+this.repetitions+" "+this.endsWith+" "+this.isPrimaryKey;
	}
}
