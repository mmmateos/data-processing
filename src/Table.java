import util.Setting;

import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class Table {

	private String schemaName;                              // Schema name
	private String name;                                    // Table name
	private List<Column> columnList = new ArrayList<>();    // All columns in the table
	private List<Column> primaryKey = new ArrayList<>();    // The most likely PK (PK can be a composite -> List)

	public Table(String schemaName, String name, List<Column> columnList, List<Column> primaryKey) {
		this.schemaName = schemaName;
		this.name = name;
		this.columnList = columnList;
		this.primaryKey = primaryKey;
	}

	public static String getHeader() {
		return String.join(Setting.CSV_SEPARATOR,
				"schema",
				"table",
				"column",
				"dataTypeName",
				"isUnique",
				"isUniqueConstraint",
				"columnSize",
				"decimalDigits",
				"hasDefault",
				"ordinalPosition",
				"ordinalPositionEnd",
				"tableColumnCount",
				"isAutoincrement",
				"isGeneratedColumn",
				"isNullable",
				"levenshteinDistance",
				"repetitions",
				"prefixSchemaCount",
				"suffixSchemaCount",
				"prefixTableCount",
				"suffixTableCount",
				"prefixRatio",
				"suffixRatio",
				"containsNo",
				"containsCode",
				"containsAux",
				"containsName",
				"containsSk",
				"containsId",
				"containsPk",
				"containsType",
				"containsKey",
				"containsNbr",
				"contains",
				"isPrimaryKey"
		);
	}

	public String getName() {
		return name;
	}

	public List<Column> getColumnList() {
		return columnList;
	}

	public List<Column> getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(List<Column> primaryKey) {
		this.primaryKey = primaryKey;
	}


	public void getColumns(DatabaseMetaData database, String schemaName, String tableName) throws SQLException {
		List<Column> columns = new ArrayList<>();

		try (ResultSet result = database.getColumns(schemaName, null, tableName, null)) {
			while (result.next()) {
				Column column = new Column(result.getString("COLUMN_NAME"));
				column.setDataType(result.getInt("DATA_TYPE"));
				column.setDataTypeName(JDBCType.valueOf(result.getInt("DATA_TYPE")).toString());
				column.setNullable(result.getBoolean("NULLABLE"));
				column.setColumnSize(result.getInt("COLUMN_SIZE"));
				column.setDecimalDigits(result.getInt("DECIMAL_DIGITS"));
				column.setHasDefault(result.getString("COLUMN_DEF") != null);
				column.setOrdinalPosition(result.getInt("ORDINAL_POSITION"));
				column.setAutoincrement(result.getBoolean("IS_AUTOINCREMENT"));
				column.setGeneratedColumn(result.getBoolean("IS_GENERATEDCOLUMN"));
				columns.add(column);
			}
		}
		columnList = setCounts(columns);
	}

	private List<Column> setCounts(List<Column> columnList) {
		for (Column column : columnList) {
			column.setTableColumnCount(columnList.size());
			column.setOrdinalPositionEnd(columnList.size() - column.getOrdinalPosition() + 1);
		}
		return columnList;
	}

	public void getPrimaryKeys(DatabaseMetaData database, String schemaName, String tableName) throws SQLException {
		List<Column> primaryKeys = new ArrayList<>();

		try (ResultSet result = database.getPrimaryKeys(schemaName, schemaName, tableName)) {
			while (result.next()) {
				for (Column column : columnList) {
					if (column.getName().equals(result.getString(4))) {
						primaryKeys.add(column);
						column.setPrimaryKey(true);
					}
				}
			}
		}
		primaryKey = primaryKeys;
	}

	public void getUniqueConstraint(DatabaseMetaData database, String schemaName, String tableName) throws SQLException {
		try (ResultSet result = database.getIndexInfo(schemaName, schemaName, tableName, true, true)) {
			while (result.next()) {
				for (Column col : columnList) {
					if (col.getName().equals(result.getString(9))) {
						col.setUniqueConstraint(true);
					}
				}
			}
		}
	}


	public String toQuery() {
		String keys = "";
		for (Column column : primaryKey) {
			keys = keys + column.getName() + "`, ";
		}
		keys = keys.substring(0, keys.length() - 3);
		return "ALTER TABLE `" + name + "` ADD PRIMARY KEY (`" + keys + "`);";
	}

	public String toProbability() {
		String table = "";
		for (Column column : columnList) {
			table += String.join(Setting.CSV_SEPARATOR,
					Setting.CSV_QUOTE + schemaName + Setting.CSV_QUOTE,
					Setting.CSV_QUOTE + name + Setting.CSV_QUOTE,
					Setting.CSV_QUOTE + column.getName() + Setting.CSV_QUOTE,
					Setting.CSV_QUOTE + column.getPrimaryKeyProbability() + Setting.CSV_QUOTE
			) + "\n";
		}
		return table;
	}

	public String toString() {
		String table = "";
		for (Column column : columnList) {
			table += String.join(Setting.CSV_SEPARATOR,
					Setting.CSV_QUOTE + schemaName + Setting.CSV_QUOTE,
					Setting.CSV_QUOTE + name + Setting.CSV_QUOTE,
					column.toString()
			) + "\n";
		}
		return table;
	}


}
