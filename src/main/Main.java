package main;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Main {

	static String sourceDriver;
	static String sourceUrl;
	static String sourceUsername;
	static String sourcePassword;
	static String sourceTable;
	static Connection sourceConn;

	static String targetDriver;
	static String targetUrl;
	static String targetUsername;
	static String targetPassword;
	static String targetTable;
	static Connection targetConn;

	static int batchSize;

	public static void main(String[] args) throws Exception {
		File propertiesFile = new File("sync.properties");
		if (!propertiesFile.exists()) {
			System.out.println("Properties file : " + propertiesFile.getAbsolutePath() + "not found");
			return;
		}

		Properties properties = new Properties();
		try (InputStream is = new FileInputStream(propertiesFile)) {
			properties.load(is);
		}

		try {
			batchSize = Integer.parseInt(properties.getProperty("batch.size"));
			System.out.println("batch.size" + "=" + batchSize);
		} catch (Exception e) {
			batchSize = 1000;
			System.out.println("batch.size is not validated, set default batch size = 1000");
		}

		sourceDriver = properties.getProperty("source.driver");
		System.out.println("source.driver" + "=" + sourceDriver);
		sourceUrl = properties.getProperty("source.url");
		System.out.println("source.url" + "=" + sourceUrl);
		sourceUsername = properties.getProperty("source.username");
		System.out.println("source.username" + "=" + sourceUsername);
		sourcePassword = properties.getProperty("source.password");
		System.out.println("source.password" + "=" + sourcePassword);
		sourceTable = properties.getProperty("source.table");
		System.out.println("source.table" + "=" + sourceTable);
		Class.forName(sourceDriver);

		targetDriver = properties.getProperty("target.driver");
		System.out.println("target.driver" + "=" + targetDriver);
		targetUrl = properties.getProperty("target.url");
		System.out.println("target.url" + "=" + targetUrl);
		targetUsername = properties.getProperty("target.username");
		System.out.println("target.username" + "=" + targetUsername);
		targetPassword = properties.getProperty("target.password");
		System.out.println("target.password" + "=" + targetPassword);
		targetTable = properties.getProperty("target.table");
		System.out.println("target.table" + "=" + targetTable);
		Class.forName(targetDriver);

		System.out.println("Open Source Connection...");
		sourceConn = DriverManager.getConnection(sourceUrl, sourceUsername, sourcePassword);
		System.out.println("Connected!");

		System.out.println("Open Target Connection...");
		targetConn = DriverManager.getConnection(targetUrl, targetUsername, targetPassword);
		sourceConn.setAutoCommit(true);
		System.out.println("Connected!");

		syncDataSource(sourceTable, targetTable);
		sourceConn.close();
		targetConn.close();

	}

	private static int selectCount() throws SQLException {
		Statement statement = sourceConn.createStatement();
		ResultSet resultSet = statement.executeQuery(String.format("select count(*) from %s", sourceTable));
		resultSet.next();
		int count = resultSet.getInt(1);
		resultSet.close();
		statement.close();
		return count;
	}

	private static void syncDataSource(String sourceTable, String targetTable) throws SQLException {

		final int count = selectCount();
		String selectSQL = String.format("select * from %s", sourceTable);
		System.out.println("Execute " + selectSQL);
		Statement sourceStatement = sourceConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		sourceStatement.setFetchSize(batchSize);

		ResultSet resultSet = sourceStatement.executeQuery(selectSQL);
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		final int columnCount = resultSetMetaData.getColumnCount();

		List<String> columnNames = new ArrayList<>();
		List<Integer> columnTypes = new ArrayList<>();
		for (int i = 1; i <= columnCount; i++) {
			columnNames.add(resultSetMetaData.getColumnName(i));
			columnTypes.add(resultSetMetaData.getColumnType(i));
		}
		String sqlColumn = columnNames.stream().collect(Collectors.joining(","));
		String sqlValues = columnNames.stream().map(o -> "?").collect(Collectors.joining(","));
		String sql = String.format("insert into %s (%s) values (%s)", targetTable, sqlColumn, sqlValues);

		AtomicInteger rowCount = new AtomicInteger(0);

		while (resultSet.next()) {
			PreparedStatement statment = targetConn.prepareStatement(sql);
			for (int i = 1; i <= columnCount; i++) {
				statment.setObject(i, resultSet.getObject(i), columnTypes.get(i - 1));
			}
			System.out.println("Inserting " + targetTable + "(" + rowCount.addAndGet(1) + "/" + count + ")");
			int effectRows = statment.executeUpdate();
			statment.close();
			if (effectRows <= 0) {
				throw new RuntimeException("Effect rows = 0");
			}

			if (rowCount.get() % batchSize == 0) {

				System.out.println("due to batch size. reconnect connections.");

				sourceStatement.close();
				resultSet.close();
				sourceConn.close();
				sourceConn = DriverManager.getConnection(sourceUrl, sourceUsername, sourcePassword);
				System.out.println("Source Connected!");

				targetConn.close();
				targetConn = DriverManager.getConnection(targetUrl, targetUsername, targetPassword);
				targetConn.setAutoCommit(true);
				System.out.println("Target Connected!");

				System.gc();

				sourceStatement = sourceConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				sourceStatement.setFetchSize(batchSize);
				resultSet = sourceStatement.executeQuery(selectSQL);
				resultSet.absolute(rowCount.get());
			}
		}

		resultSet.close();
		sourceStatement.close();

	}

}
