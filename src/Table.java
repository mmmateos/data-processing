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
		this.columnList = columns;
	}

	public void getPrimaryKeys(DatabaseMetaData database, String schemaName, String tableName) throws SQLException {
		List<Column> primaryKeys = new ArrayList<>();

		for (Column col : this.columnList) {
			col.setPrimaryKey(false);
		}

		try (ResultSet result = database.getPrimaryKeys(schemaName, schemaName, tableName)) {
			while (result.next()) {
				for (Column column : this.columnList) {
					if (column.getName().equals(result.getString(4))) {
						primaryKeys.add(column);
						column.setPrimaryKey(true);
					}
				}
			}
		}
		this.primaryKey = primaryKeys;
	}

	public void getUniqueConstraint(DatabaseMetaData database, String schemaName, String tableName) throws SQLException {
		for (Column col : this.columnList) {
			col.setUniqueConstraint(false);
		}

		try (ResultSet result = database.getIndexInfo(schemaName, schemaName, tableName, true, true)) {
			while (result.next()) {
				for (Column col : this.columnList) {
					if (col.getName().equals(result.getString(9))) {
						col.setUniqueConstraint(true);
					}
				}
			}
		}
	}

	public String toString() {
		String table = "";
		for (Column column : this.columnList) {
			table += String.join(Data.CSV_SEPARATOR,
					Data.CSV_QUOTE + schemaName + Data.CSV_QUOTE,
					Data.CSV_QUOTE + name + Data.CSV_QUOTE,
					column.toString()
			) + "\n";
		}
		return table;
	}

}
