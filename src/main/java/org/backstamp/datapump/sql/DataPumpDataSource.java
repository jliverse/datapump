package org.backstamp.datapump.sql;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.backstamp.datapump.DataPumpFile;
import org.backstamp.datapump.DataPumpTable;
import org.backstamp.datapump.row.MasterTableRow;
import org.backstamp.datapump.row.TableRow;

public class DataPumpDataSource implements DataSource {
	private Connection connection;
	private DataPumpFile dataPumpFile;
	private String[] tables;
	
	public DataPumpDataSource(DataPumpFile dataPumpFile, String... tables) {
		this.dataPumpFile = dataPumpFile;
		this.tables = tables;
	}

	/* (non-Javadoc)
	 * @see javax.sql.DataSource#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return getConnection("sa", "sa");
	}

	/* (non-Javadoc)
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		if (connection == null) {
			connection = DriverManager.getConnection(
				"jdbc:h2:mem:datapump;db_close_delay=-1;mvcc=true",
				username,
				password);
			for (int i = 0; i < tables.length; i++) {
				populateTable(tables[i]);
			}
		}
		return connection;
	}

	@Deprecated
	private int[] populateTable(String name) throws SQLException {
		return populateTable(row -> row.objectName().equalsIgnoreCase(name));
	}

	private int[] populateTable(Predicate<MasterTableRow> predicate) throws SQLException {
		DataPumpTable<TableRow> table = dataPumpFile.tableMatching(predicate);

		// CREATE TABLE ...
		try (PreparedStatement s = connection.prepareStatement(table.get().toSQL());) {
			s.executeUpdate();
		}
		
		// INSERT INTO ...
		try (PreparedStatement s = connection.prepareStatement(table.get().toSQLInsertSyntax())) {
			Stream<TableRow> rowStream = table.rows();
			rowStream.forEach(row -> {
				try {
					row.populateStatement(s);
					s.addBatch();
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			});
			return s.executeBatch(); 
		}
	}

	@Override
	public PrintWriter getLogWriter() throws SQLException { return null; }

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {}

	@Override
	public int getLoginTimeout() throws SQLException { return 0; }

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException { return null; }

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException(); }

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }
}
