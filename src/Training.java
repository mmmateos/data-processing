import util.Setting;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;


public class Training {


	public static void main(String[] args) {
		String query = "SELECT DISTINCT TABLE_SCHEMA FROM information_schema.columns"
				+ " WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'predictor_factory', 'mysql', 'meta', 'Phishing', 'fairytale')"
				+ " AND TABLE_SCHEMA NOT LIKE 'arnaud_%' AND TABLE_SCHEMA NOT LIKE 'ctu_%'";

		try (Connection connection = Setting.getDataSource().getConnection();
		     Statement stmt = connection.createStatement();
		     ResultSet result = stmt.executeQuery(query)) {

			DatabaseMetaData database = connection.getMetaData();

			try (PrintWriter writer = new PrintWriter("estimatePK_v3.csv", "UTF-8")) {
				writer.println(Table.getHeader());
				while (result.next()) {
					String schemaName = result.getString(1);
					List<Table> schema = Feature.getFeatures(database, schemaName);
					schema = Feature.getExpensiveFeatures(schema, connection, schemaName);

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
