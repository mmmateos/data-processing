import com.rits.cloning.Cloner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Feature {

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

	public static List<Table> getFeatures(DatabaseMetaData database, String schemaName) throws SQLException {
		List<Table> schema = getTables(database, schemaName);

		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				column.setLD(table.getName());
				column.setKeywords();
				column.setRepetition(schema);
				column.setPrefixSchemaCount(schema);
				column.setSuffixSchemaCount(schema);
				column.setPrefixTableCount(table);
				column.setSuffixTableCount(table);
			}
		}

		return schema;
	}

	public static List<Table> getExpensiveFeatures(List<Table> schema, Connection connection, String schemaName) throws SQLException {
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				column.isUnique(connection, schemaName, table.getName());
			}
		}
		return schema;
	}


	public static List<Relationship> getRelationships(DatabaseMetaData database, String schemaName, List<Table> schema) throws SQLException {
		List<Relationship> relationships = generateRelationships(schema);

		for (Table table : schema) {
			try (ResultSet result = database.getImportedKeys(schemaName, schemaName, table.getName())) {
				while (result.next()) {
					String pkTable = result.getString("PKTABLE_NAME");
					String fkTable = result.getString("FKTABLE_NAME");
					String pkColumn = result.getString("PKCOLUMN_NAME");
					String fkColumn = result.getString("FKCOLUMN_NAME");

					for (Relationship relationship : relationships) {
						if (fkColumn.equals(relationship.getFk().getName()) &&
								pkColumn.equals(relationship.getPk().getName()) &&
								fkTable.equals(relationship.getFkTable()) &&
								pkTable.equals(relationship.getPkTable())) {
							relationship.setForeignKey(true);
						}
					}
				}
			}
		}

		return relationships;
	}

	private static List<Relationship> generateRelationships(List<Table> schema) {
		// Generate list of all fk: table.column
		List<Relationship> nList = new ArrayList<>();
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				Relationship relationship = new Relationship();
				relationship.setFk(column);
				relationship.setFkTable(table.getName());
				nList.add(relationship);
			}
		}

		// Generate list of all table.pk---table.column
		// Note there is only 1 PK per table.
		Cloner cloner = new Cloner();
		List<Relationship> nnList = new ArrayList<>();
		for (Table pkTable : schema) {
			if (!pkTable.getPrimaryKey().isEmpty()) {
				Column pk = pkTable.getPrimaryKey().get(0);
				List<Relationship> cloned = cloner.deepClone(nList);
				for (Relationship relationship : cloned) {
					relationship.setPk(pk);
					relationship.setPkTable(pkTable.getName());
				}
				nnList.addAll(cloned);
			}
		}

		return nnList;
	}
}
