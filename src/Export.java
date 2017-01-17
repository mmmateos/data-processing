import util.Setting;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class Export {

	public static String getQueries(List<Table> schema) {
		String queries = "";
		for (Table table : schema) {
			queries = queries + table.toQuery() + "\n";
		}
		return queries;
	}

	private static String getHeader() {
		return String.join(Setting.CSV_SEPARATOR,
				"schema",
				"table",
				"column",
				"primaryKeyProbability"
		);
	}

	public static void writeCSV(List<Table> schema) throws FileNotFoundException, UnsupportedEncodingException {
		try (PrintWriter writer = new PrintWriter("probability.csv", "UTF-8")) {
			writer.println(getHeader());
			for (Table table : schema) {
				writer.print(table.toProbability());
			}
		}
	}


}
