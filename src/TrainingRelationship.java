import util.Setting;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;


public class TrainingRelationship {


	public static void main(String[] args) {
		String query = "SELECT DISTINCT TABLE_SCHEMA FROM information_schema.columns"
				+ " WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'predictor_factory', 'mysql', 'meta', 'Phishing', 'fairytale')"
				+ " AND TABLE_SCHEMA NOT LIKE 'arnaud_%' AND TABLE_SCHEMA NOT LIKE 'ctu_%'";


		try (Connection connection = Setting.getDataSource().getConnection();
		     Statement stmt = connection.createStatement();
			 ResultSet result = stmt.executeQuery(query)) {

			try (PrintWriter writer = new PrintWriter("estimateFK_v3.csv", "UTF-8")) {
				writer.println(Relationship.getHeader());

				while (result.next()) {
					String schemaName = result.getString(1);

					// Get columns
					DatabaseMetaData database = connection.getMetaData();
					List<Table> schema = Feature.getFeatures(database, schemaName);

					// Get relations
					List<Relationship> relationships = Feature.getRelationships(database, schemaName, schema);

					// Set features
					for (Relationship relationship : relationships) {
						relationship.setAgree();
						relationship.setLD();
						relationship.setKeywords();
						relationship.setSchema(schemaName);
					}

					// Print
					for (Relationship relationship : relationships) {
						writer.println(relationship.toString());
					}
					System.out.println(schemaName);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
