/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package constant;

import java.sql.Connection;
import javax.sql.DataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 *
 * @author DucHuynh
 */
public class ConnectionPool {

    public static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "root";
    public static DataSource dataSource;
    private GenericObjectPool connectionPool = null;

    public ConnectionPool(String username, String password, String database, int port) {
        try {
            database = "jdbc:mysql://localhost:" + port + "/" + database;
            System.out.println(database);
            //Load JDBC Driver to connect to MySQL
            Class.forName(ConnectionPool.DRIVER).newInstance();
            // Create a object to hold my pool.
            connectionPool = new GenericObjectPool();
            connectionPool.setMaxActive(10);
            //
            // Creates a connection factory object which will be use by
            // the pool to create the connection object. We passes the
            // JDBC url info, username and password.
            //
            ConnectionFactory cf = new DriverManagerConnectionFactory(
                    database,
                    username,
                    password);
            PoolableConnectionFactory pcf = new PoolableConnectionFactory(
                    cf,
                    connectionPool,
                    null,
                    null,
                    false,
                    true);
            dataSource = new PoolingDataSource(connectionPool);

        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    public Connection getConnection() {
        try {
            Connection con = dataSource.getConnection();
            return con;
        } catch (Exception e) {
            System.out.println(e.toString());
            return null;
        }
    }
}