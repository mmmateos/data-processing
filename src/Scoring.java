import util.Setting;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Scoring {

	public static void main(String[] arg) throws FileNotFoundException, UnsupportedEncodingException {
		long startTime = System.nanoTime();

		// Collect metadata
		// The metadata should be collected at schema level to minimize the count of round trips.
		// However, getPrimaryKeys() and getColumns() work only on the table and column level.
		String schemaName = "Toxicology";
		List<Table> schema = new ArrayList<>();
		List<Relationship> relationships = new ArrayList<>();
		try (Connection connection = Setting.getDataSource().getConnection()) {
			DatabaseMetaData database = connection.getMetaData();
			schema = Feature.getFeatures(database, schemaName);
			relationships = Feature.getRelationships(database, schemaName, schema);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Set probabilities
		for (Table table : schema) {
			for (Column column : table.getColumnList()) {
				column.estimatePrimaryKeyProbability();
			}
		}

		// Set most likely PKs
		for (Table table : schema) {
			double maximum = 0;
			for (Column column : table.getColumnList()) {
				if (column.getPrimaryKeyProbability() > maximum) {
					maximum = column.getPrimaryKeyProbability();
					table.setPrimaryKey(Arrays.asList(column));
				}
			}
		}


		// Set probabilities FK
		for (Relationship relationship : relationships) {
			relationship.setAgree();
			relationship.setLD();
			relationship.setKeywords();
			relationship.setSchema(schemaName);

			relationship.estimateForeignKeyProbability();
		}

		// Set most likely FK
		for (Relationship relationship : relationships) {
			if (relationship.getForeignKeyProbability() > 0.5) {
				relationship.setForeignKey(true);
			}
		}

		// Export the result
		System.out.println(Export.getQueries(schema));
		for (Relationship relationship : relationships) {
			if (relationship.getForeignKey()) {
				System.out.println(relationship.toQuery());
			}
		}

		Export.writeCSV(schema);

		// Bye!
		long elapsedTime = System.nanoTime() - startTime;
		System.out.println();
		System.out.println("The processing took " + TimeUnit.NANOSECONDS.toMillis(elapsedTime) + " ms.");
	}
}
