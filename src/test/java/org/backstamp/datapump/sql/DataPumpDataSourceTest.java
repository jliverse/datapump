package org.backstamp.datapump.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.backstamp.datapump.DataPumpFile;
import org.backstamp.datapump.io.TemporaryFile;

import junit.framework.TestCase;

public class DataPumpDataSourceTest extends TestCase {

	/**
	 * For an Oracle<sup>TM</sup>-type dump file (i.e., generated via <code>expdp</code>) can
	 * <pre>
	 * be parsed
	 * have selected tables (e.g., DEPT and EMP) loaded into an H2 in-memory database
	 * have SQL queries run against the exported table data
	 * that return correct results.
	 * </pre>
	 * Oracle is a registered trademark of Oracle Corporation and/or its affiliates.
	 * @throws Exception
	 */
	public void testScott() throws Exception {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		try (TemporaryFile file = new TemporaryFile(cl.getResourceAsStream("scott.dmp"));) {

			DataPumpFile dumpFile = new DataPumpFile(file.toFile());
			DataPumpDataSource dataSource = new DataPumpDataSource(dumpFile, "DEPT", "EMP");

			String sql = "SELECT ename"
				+ "  FROM emp e"
				+ "  JOIN dept d ON d.deptno = e.deptno"
				+ " WHERE d.loc = 'CHICAGO'"
				+ " ORDER BY 1";

			try (Connection connection = dataSource.getConnection();
				Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery(sql);) {
				List<String> results = new ArrayList<>();
				while (rs.next()) {
					results.add(rs.getString(1));
				}
				assertEquals("[ALLEN, BLAKE, JAMES, MARTIN, TURNER, WARD]", results.toString());
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
