import util.Levenshtein;
import util.Logistic;
import util.Setting;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Relationship {

	private static final List<String> KEYWORDS_FK = Arrays.asList("fk", "type", "eid");
	private static final double[] WEIGHTS = new double[]{
			-2.134651321,
			-0.353421656,
			0.07738626,
			-0.027985874,
			-0.543985685,
			2.949503824,
			1.989662572,
			-0.396326589};
	private static final double BIAS = -5.990210353;

//	1) Do the data types agree?
//	2) Do the data type properties (like length and count of decimals) agree?
//	3) Levenshtein distance between the primary key and the attribute name.
//	4) Does the attribute name end with (case insensitive): {aux, code, id, key, name, nbr, no, pk, sk, type}? Each suffix should also be appended with "\d*" to permit numbered suffixes (for the matching use regex). Create one feature for each suffix.
//	5) Contains null value? (As checked with a query on the actual data.)
//	6) If we randomly select N (or top N) unique records, what proportion of them can we match to the primary key? N can be e.g. 500.
//	7) Table row count comparison (of the table with the PK and the table with the attribute).
// + Other attributes from PK detection
	private Boolean dataTypeAgree;
	private Boolean dataTypeCategoryAgree;
	private Boolean dataLengthAgree;
	private Boolean decimalAgree;
	private Integer levenshteinColumns;
	private Map<String, Boolean> containsFKName;
	private Column fk;                              // Should permit composite fk & pk
	private Column pk;
	private String fkTable;
	private String pkTable;
	private String schema;
	private Double foreignKeyProbability;           // Estimated probability that this is a foreign key
	private Boolean isForeignKey = false;           // The label


	public static String getHeader() {
		return String.join(Setting.CSV_SEPARATOR,
				"pkTable",
				Table.getHeader(),
				"fkTable",
				Table.getHeader(),
				"dataTypeAgree",
				"dataTypeCategoryAgree",
				"dataLengthAgree",
				"decimalAgree",
				"levenshteinColumns",
				"containsFk",
				"containsType",
				"containsEid",
				"contains",
				"isForeignKey"
		);
	}



	public void setForeignKey(Boolean foreignKey) {
		isForeignKey = foreignKey;
	}

	public Column getFk() {
		return fk;
	}

	public void setFk(Column fk) {
		this.fk = fk;
	}

	public Column getPk() {
		return pk;
	}

	public String getFkTable() {
		return fkTable;
	}

	public void setFkTable(String fkTable) {
		this.fkTable = fkTable;
	}

	public String getPkTable() {
		return pkTable;
	}

	public void setPkTable(String pkTable) {
		this.pkTable = pkTable;
	}

	public Double getForeignKeyProbability() {
		return foreignKeyProbability;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public Boolean getForeignKey() {
		return isForeignKey;
	}

	public void setPk(Column pk) {
		this.pk = pk;
	}


	public void setAgree() {
		dataTypeAgree = (pk.getDataType() == fk.getDataType());
		dataTypeCategoryAgree = getDataTypeCategory(pk.getDataTypeName()).equals(getDataTypeCategory(fk.getDataTypeName()));
		dataLengthAgree = (pk.getColumnSize() == fk.getColumnSize());
		decimalAgree = (pk.getDecimalDigits() == fk.getDecimalDigits());
	}

	private String getDataTypeCategory(String dataTypeName) {
		if ("CHAR".equals(dataTypeName)) return "CHAR";
		if ("VARCHAR".equals(dataTypeName)) return "CHAR";
		if ("LONGVARCHAR".equals(dataTypeName)) return "CHAR";

		if ("INTEGER".equals(dataTypeName)) return "INTEGER";
		if ("BIGINT".equals(dataTypeName)) return "INTEGER";
		if ("SMALLINT".equals(dataTypeName)) return "INTEGER";

		if ("DOUBLE".equals(dataTypeName)) return "DOUBLE";
		if ("REAL".equals(dataTypeName)) return "DOUBLE";

		return dataTypeName;
	}

	public void setLD() {
		levenshteinColumns = Levenshtein.getDistance(fk.getName(), pk.getName());
	}

	public void setKeywords() {
		containsFKName = util.Tokenization.contains(fk.getName(), KEYWORDS_FK);
	}

	public void estimateForeignKeyProbability() {
		foreignKeyProbability = Logistic.classify(toArray(), WEIGHTS, BIAS);
	}



	private double[] toArray() {
		return new double[]{
				fk.getPrimaryKey() ? 1 : 0,
				fk.getContains().containsValue(true) ? 1 : 0,
				fk.getLD(),
				fk.getOrdinalPosition(),
				dataTypeAgree ? 1 : 0,
				dataTypeCategoryAgree ? 1 : 0,
				dataLengthAgree ? 1 : 0,
				levenshteinColumns,
				isForeignKey ? 1 : 0,
		};
	}

	public String toQuery() {
		String fk = this.fk.getName();
		String pk = this.pk.getName();
		return "ALTER TABLE `" + fkTable + "` ADD FOREIGN KEY (`" + fk + "`) REFERENCES `" + pkTable + "`(`" + pk + "`);";
	}

	public String toString() {
		return String.join(Setting.CSV_SEPARATOR,
				Setting.CSV_QUOTE + schema + Setting.CSV_QUOTE,
				Setting.CSV_QUOTE + pkTable + Setting.CSV_QUOTE,
				pk.toString(),
				Setting.CSV_QUOTE + fkTable + Setting.CSV_QUOTE,
				fk.toString(),
				dataTypeAgree ? "true" : "false",
				dataTypeCategoryAgree ? "true" : "false",
				dataLengthAgree ? "true" : "false",
				decimalAgree ? "true" : "false",
				levenshteinColumns.toString(),
				containsFKName.values().stream().map(Object::toString).collect(Collectors.joining(Setting.CSV_SEPARATOR)),
				containsFKName.containsValue(true) ? "true" : "false",
				isForeignKey.toString()
		);
	}
}
