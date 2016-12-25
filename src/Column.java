import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Column {

	private int dataType;                   // Data type as defined by JDBC
    private String dataTypeName;            // Data type as defined by JDBC
    private boolean isUnique;               // From query
    private boolean isUniqueConstraint;     // From getIndexInfo()
	private boolean isNullable;             // From getColumns()
	private int columnSize;
	private int decimalDigits;
	private boolean hasDefault;
	private int ordinalPosition;
	private boolean isAutoincrement;
	private boolean isGeneratedColumn;
    private String name;                    // Column name
    private Map<String,Boolean> endsWith;  	// Map of booleans of columnName endings
    private int levenshteinDistance;		// Levenshtein Distance columnName vs. tableName
    private int repetitions;				// Count of columnName repetitions at the schema level
    private double primaryKeyProbability;   // Estimated probability that this column alone is a PK
    private boolean isPrimaryKey;           // The label


    public Column(String name) {
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

	public boolean isUniqueConstraint() {
		return isUniqueConstraint;
	}

	public void setUniqueConstraint(boolean uniqueConstraint) {
		isUniqueConstraint = uniqueConstraint;
	}

	public int getColumnSize() {
		return columnSize;
	}

	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}

	public int getDecimalDigits() {
		return decimalDigits;
	}

	public void setDecimalDigits(int decimalDigits) {
		this.decimalDigits = decimalDigits;
	}

	public boolean hasDefault() {
		return hasDefault;
	}

	public void setHasDefault(boolean hasDefault) {
		this.hasDefault = hasDefault;
	}

	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	public void setOrdinalPosition(int ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}

	public boolean isAutoincrement() {
		return isAutoincrement;
	}

	public void setAutoincrement(boolean autoincrement) {
		isAutoincrement = autoincrement;
	}

	public boolean isGeneratedColumn() {
		return isGeneratedColumn;
	}

	public void setGeneratedColumn(boolean generatedColumn) {
		isGeneratedColumn = generatedColumn;
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

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}


	// TODO: Add reference to the implementation
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
		String[] ends = {"aux","code","id","key","name","nbr","no","pk","sk","type"};
		Map<String,Boolean> endWith = new HashMap<>();

		for (String end : ends) {
			if (this.name.endsWith(end))
				endWith.put(end, true);
			else
				endWith.put(end, false);
		}
		this.endsWith = endWith;
	}

	public void calculateReps(List<Table> schema){
		int cont=0;
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				if (this.name.equals(column.getName()))
					cont++;
			}
		}
		this.repetitions = cont;
	}

	public void isUnique(Connection conn, String schemaName, String tableName) throws SQLException{
		String query = "select count(`"+this.name+"`) - count(distinct `"+this.name+"`) from `" + schemaName + "`.`" + tableName + "`";

		try (Statement stmt = conn.createStatement();
		     ResultSet rs = stmt.executeQuery(query)) {
			if(rs.next()){
				this.isUnique = (rs.getInt(1) == 0);
			}
		}
	}

	// TODO: This should be done at the table level because it returns a resultSet for the whole table.
	// TODO: The unique constraint should be set for the specific column. Not all columns.
	public void isUniqueConstraint(DatabaseMetaData database, String schemaName, String tableName) throws SQLException{
		try  (ResultSet result = database.getIndexInfo(schemaName, schemaName, tableName, true, true)) {
			while (result.next()) {
				this.isUniqueConstraint = result.getBoolean("NON_UNIQUE");
			}
		}
	}

	public String toString(){
		return this.name+"\t"+this.dataTypeName+"\t"+this.isUnique+"\t"+this.isUniqueConstraint+"\t"+
				this.columnSize+"\t"+this.decimalDigits+"\t"+this.hasDefault+"\t"+this.ordinalPosition+"\t"+
				this.isAutoincrement+"\t"+this.isGeneratedColumn+"\t"+this.isNullable+"\t"+
				this.levenshteinDistance+"\t"+this.repetitions+"\t"+this.endsWith+"\t"+this.isPrimaryKey;
	}
}
