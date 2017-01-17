import util.Logistic;
import util.Setting;
import util.Tokenization;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Column {
	private static final List<String> KEYWORDS = Arrays.asList("aux", "code", "id", "key", "name", "nbr", "no", "pk", "sk", "type");
	private static final double[] WEIGHTS = new double[]{
			1.881462417,
			1.113801441,
			1.839534369,
			1.490454794,
			1.119517412,
			-2.30306826,
			-0.760086066,
			-0.101100801,
			-0.030199945,
			-0.096808844,
			0.049097089,
			0.140770431,
			-0.398085495,
			0.11258666,
			-0.036447508,
			2.739129646};
	private static final double BIAS = 1.654664792;

	private Integer dataType;               // Data type as defined by JDBC
	private String dataTypeName;            // Data type as defined by JDBC
	private Boolean isUnique = false;
	private Boolean isUniqueConstraint = false;  // From getIndexInfo()
	private Boolean isNullable;             // From getColumns()
	private Integer columnSize;
	private Integer decimalDigits;
	private Boolean hasDefault;
	private Integer ordinalPosition;
	private Integer ordinalPositionEnd;
	private Integer tableColumnCount;
	private Boolean isAutoincrement;
	private Boolean isGeneratedColumn;
	private String name;                    // Column name
	private Map<String, Boolean> contains;  // Map of booleans of columnName endings
	private Integer levenshteinDistance;    // Levenshtein Distance columnName vs. tableName
	private Integer repetitions;            // Count of columnName repetitions at the schema level
	private Integer prefixSchemaCount;      // Count of occurrences of the given column name prefix in the schema
	private Integer suffixSchemaCount;      // Count of occurrences of the given column name suffix in the schema
	private Integer prefixTableCount;       // Count of occurrences of the given column name prefix in the table
	private Integer suffixTableCount;       // Count of occurrences of the given column name suffix in the table
	private Double primaryKeyProbability;   // Estimated probability that this column alone is a PK
	private Boolean isPrimaryKey = false;   // The label


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

	public void setNullable(boolean isNullable) {
		this.isNullable = isNullable;
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

	public void setHasDefault(boolean hasDefault) {
		this.hasDefault = hasDefault;
	}

	public void setOrdinalPosition(int ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}

	public Integer getOrdinalPosition() {
		return ordinalPosition;
	}

	public void setOrdinalPositionEnd(Integer ordinalPositionEnd) {
		this.ordinalPositionEnd = ordinalPositionEnd;
	}

	public void setTableColumnCount(Integer tableColumnCount) {
		this.tableColumnCount = tableColumnCount;
	}

	public void setAutoincrement(boolean autoincrement) {
		isAutoincrement = autoincrement;
	}

	public void setGeneratedColumn(boolean generatedColumn) {
		isGeneratedColumn = generatedColumn;
	}

	public String getName() {
		return name;
	}

	public double getPrimaryKeyProbability() {
		return primaryKeyProbability;
	}

	public Map<String, Boolean> getContains() {
		return contains;
	}

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public Boolean getPrimaryKey() {
		return isPrimaryKey;
	}


	public void setLD(String a) {
		levenshteinDistance = util.Levenshtein.getDistance(a, name);
	}

	public Integer getLD() {
		return levenshteinDistance;
	}

	public void setKeywords() {
		contains = Tokenization.contains(name, KEYWORDS);
	}

	public void setRepetition(List<Table> schema) {
		int cont = 0;
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				if (name.equals(column.getName()))
					cont++;
			}
		}
		repetitions = cont;
	}

	public void setPrefixSchemaCount(List<Table> schema) {
		if (name.length() < 3) {
			prefixSchemaCount = 1;
			return;
		}

		int cont = 0;
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				if (column.getName().length() > 2) {
					if (name.substring(0, 3).equals(column.getName().substring(0, 3)))
						cont++;
				}
			}
		}
		prefixSchemaCount = cont;
	}

	public void setPrefixTableCount(Table table) {
		if (name.length() < 3) {
			prefixTableCount = 1;
			return;
		}

		int cont = 0;
		for (Column column : table.getColumnList()) {
			if (column.getName().length() > 2) {
				if (name.substring(0, 3).equals(column.getName().substring(0, 3)))
					cont++;
			}
		}

		prefixTableCount = cont;
	}

	public void setSuffixSchemaCount(List<Table> schema) {
		if (name.length() < 3) {
			suffixSchemaCount = 1;
			return;
		}

		int cont = 0;
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				if (column.getName().length() > 2) {
					if (name.substring(name.length() - 3, name.length())
							.equals(column.getName().substring(column.getName().length() - 3, column.getName().length())))
						cont++;
				}
			}
		}
		suffixSchemaCount = cont;
	}

	public void setSuffixTableCount(Table table) {
		if (name.length() < 3) {
			suffixTableCount = 1;
			return;
		}

		int cont = 0;
		for (Column column : table.getColumnList()) {
			if (column.getName().length() > 2) {
				if (name.substring(name.length() - 3, name.length())
						.equals(column.getName().substring(column.getName().length() - 3, column.getName().length())))
					cont++;
			}
		}

		suffixTableCount = cont;
	}

	// Should also check not null
	public void isUnique(Connection conn, String schemaName, String tableName) throws SQLException {
		String query = "select count(`" + name + "`) - count(distinct `" + name + "`) from `" + schemaName + "`.`" + tableName + "`";

		try (Statement stmt = conn.createStatement();
		     ResultSet rs = stmt.executeQuery(query)) {
			if (rs.next()) {
				isUnique = (rs.getInt(1) == 0);
			}
		}
	}

	public void estimatePrimaryKeyProbability() {
		primaryKeyProbability = Logistic.classify(toArray(), WEIGHTS, BIAS);
	}


	private double[] toArray() {
		return new double[]{
				"INTEGER".equals(dataTypeName) ? 1 : 0,
				"VARCHAR".equals(dataTypeName) ? 1 : 0,
				"BIGINT".equals(dataTypeName) ? 1 : 0,
				"SMALLINT".equals(dataTypeName) ? 1 : 0,
				"TINYINT".equals(dataTypeName) ? 1 : 0,
				decimalDigits,
				ordinalPosition,
				levenshteinDistance,
				repetitions,
				prefixSchemaCount,
				suffixSchemaCount,
				prefixTableCount,
				suffixTableCount,
				(double) prefixSchemaCount / (double) prefixTableCount,
				(double) suffixSchemaCount / (double) suffixTableCount,
				contains.containsValue(true) ? 1 : 0
		};
	}

	public String toString() {
		return String.join(Setting.CSV_SEPARATOR,
				Setting.CSV_QUOTE + name + Setting.CSV_QUOTE,
				dataTypeName,
				isUnique.toString(),
				isUniqueConstraint.toString(),
				columnSize.toString(),
				decimalDigits.toString(),
				hasDefault.toString(),
				ordinalPosition.toString(),
				ordinalPositionEnd.toString(),
				tableColumnCount.toString(),
				isAutoincrement.toString(),
				isGeneratedColumn.toString(),
				isNullable.toString(),
				levenshteinDistance.toString(),
				repetitions.toString(),
				prefixSchemaCount.toString(),
				suffixSchemaCount.toString(),
				prefixTableCount.toString(),
				suffixTableCount.toString(),
				String.valueOf((double) prefixSchemaCount / (double) prefixTableCount),
				String.valueOf((double) suffixSchemaCount / (double) suffixTableCount),
				contains.values().stream().map(Object::toString).collect(Collectors.joining(Setting.CSV_SEPARATOR)),
				contains.containsValue(true) ? "true" : "false",
				isPrimaryKey.toString()
		);
	}
}
