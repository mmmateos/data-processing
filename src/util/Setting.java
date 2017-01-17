package util;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

import javax.sql.DataSource;

public class Setting {
	public static final String CSV_SEPARATOR = ",";
	public static final String CSV_QUOTE = "\"";

	public static DataSource getDataSource() {
		MysqlConnectionPoolDataSource dataSource = new MysqlConnectionPoolDataSource();
		dataSource.setUrl("jdbc:mysql://relational.fit.cvut.cz");
		dataSource.setUser("guest");
		dataSource.setPassword("relational");

//		dataSource.setUrl("jdbc:mysql://localhost");
//		dataSource.setUser("root");
//		dataSource.setPassword("asdasd");

		return dataSource;
	}
}
