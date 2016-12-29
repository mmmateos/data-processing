import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Column {

	private Integer dataType;               // Data type as defined by JDBC
	private String dataTypeName;            // Data type as defined by JDBC
	private Boolean isUnique;               // From query
	private Boolean isUniqueConstraint;     // From getIndexInfo()
	private Boolean isNullable;             // From getColumns()
	private Integer columnSize;
	private Integer decimalDigits;
	private Boolean hasDefault;
	private Integer ordinalPosition;
	private Boolean isAutoincrement;
	private Boolean isGeneratedColumn;
	private String name;                    // Column name
	private Map<String, Boolean> endsWith;  // Map of booleans of columnName endings
	private Integer levenshteinDistance;    // Levenshtein Distance columnName vs. tableName
	private Integer repetitions;            // Count of columnName repetitions at the schema level
	private Double primaryKeyProbability;   // Estimated probability that this column alone is a PK
	private Boolean isPrimaryKey;           // The label


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

	// Source: https://rosettacode.org/wiki/Levenshtein_distance#Java
	public void calculateLD(String a) {
		String b = this.name;

		a = a.toLowerCase();
		b = b.toLowerCase();
		int[] costs = new int[b.length() + 1];
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

	public void checkEnds() {
		String[] ends = {"aux", "code", "id", "key", "name", "nbr", "no", "pk", "sk", "type"};
		Map<String, Boolean> endWith = new HashMap<>();

		for (String end : ends) {
			if (this.name.endsWith(end))
				endWith.put(end, true);
			else
				endWith.put(end, false);
		}
		this.endsWith = endWith;
	}

	public void calculateReps(List<Table> schema) {
		int cont = 0;
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				if (this.name.equals(column.getName()))
					cont++;
			}
		}
		this.repetitions = cont;
	}

	public void isUnique(Connection conn, String schemaName, String tableName) throws SQLException {
		String query = "select count(`" + this.name + "`) - count(distinct `" + this.name + "`) from `" + schemaName + "`.`" + tableName + "`";

		try (Statement stmt = conn.createStatement();
		     ResultSet rs = stmt.executeQuery(query)) {
			if (rs.next()) {
				this.isUnique = (rs.getInt(1) == 0);
			}
		}
	}

	public String toString() {
		return String.join(Data.CSV_SEPARATOR,
				Data.CSV_QUOTE + name + Data.CSV_QUOTE,
				dataTypeName,
				isUnique.toString(),
				isUniqueConstraint.toString(),
				columnSize.toString(),
				decimalDigits.toString(),
				hasDefault.toString(),
				ordinalPosition.toString(),
				isAutoincrement.toString(),
				isGeneratedColumn.toString(),
				isNullable.toString(),
				levenshteinDistance.toString(),
				repetitions.toString(),
				endsWith.values().stream().map(Object::toString).collect(Collectors.joining(Data.CSV_SEPARATOR)),
				endsWith.containsValue(true)?"true":"false",
				isPrimaryKey.toString()
		);
	}
}
