import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class Data {
	static final String CSV_SEPARATOR = ",";
	static final String CSV_QUOTE = "\"";

	private static List<Table> getTables(DatabaseMetaData database, String schemaName) throws SQLException {
		List<Table> tables = new ArrayList<>();
		String[] types = {"TABLE"};

		try (ResultSet result = database.getTables(schemaName, null, null, types)) {
			while (result.next()) {
				String tableName = result.getString("TABLE_NAME");
				Table table = new Table(schemaName, tableName, null, null);
				table.getColumns(database, schemaName, tableName);
				table.getPrimaryKeys(database, schemaName, tableName);
				table.getUniqueConstraint(database, schemaName, tableName);
				tables.add(table);
			}
		}

		return tables;
	}

	private static List<Table> getFeatures(DatabaseMetaData database, Connection connection, String schemaName) throws SQLException {
		List<Table> schema = getTables(database, schemaName);

		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				column.isUnique(connection, schemaName, table.getName());
				column.calculateLD(table.getName());
				column.checkEnds();
				column.calculateReps(schema);
			}
		}

		return schema;
	}

	private static String getHeader() {
		return String.join(Data.CSV_SEPARATOR,
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
				"isAutoincrement",
				"isGeneratedColumn",
				"isNullable",
				"levenshteinDistance",
				"repetitions",
				"endsWithNo",
				"endsWithCode",
				"endsWithAux",
				"endsWithName",
				"endsWithSk",
				"endsWithId",
				"endsWithPk",
				"endsWithType",
				"endsWithKey",
				"endsWithNbr",
				"endsWith",
				"isPrimaryKey"
		);
	}

	private static DataSource getDataSource() {
		MysqlConnectionPoolDataSource dataSource = new MysqlConnectionPoolDataSource();
		dataSource.setUrl("jdbc:mysql://relational.fit.cvut.cz");
		dataSource.setUser("guest");
		dataSource.setPassword("relational");

		return dataSource;
	}

	public static void main(String[] args) {
		String query = "select distinct TABLE_SCHEMA from information_schema.columns"
				+ " where TABLE_SCHEMA not in ('information_schema', 'performance_schema', 'predictor_factory', 'mysql', 'meta', 'Phishing', 'fairytale')"
				+ " and TABLE_SCHEMA not like 'arnaud_%' and TABLE_SCHEMA not like 'ctu_%'";

		try (Connection connection = getDataSource().getConnection();
		     Statement stmt = connection.createStatement();
		     ResultSet result = stmt.executeQuery(query)) {

			DatabaseMetaData database = connection.getMetaData();

			try (PrintWriter writer = new PrintWriter("data.ods", "UTF-8")) {
				writer.println(getHeader());
				while (result.next()) {
					String schemaName = result.getString(1);
					List<Table> schema = getFeatures(database, connection, schemaName);

					for (Table table : schema) {
						writer.print(table.toString());
						writer.flush();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
