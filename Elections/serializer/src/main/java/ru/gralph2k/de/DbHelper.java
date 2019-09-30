package ru.gralph2k.de;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.*;

public class DbHelper implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(DbHelper.class);

    Connection c;

    public DbHelper(String user, String password, String connectionString) {
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                .getConnection(connectionString,
                    user, password);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        log.info("Opened database successfully");
    }

    public void executeUpdate(String sql)  {
        try {
            log.debug("execute {}", sql);
            Statement stmt = c.createStatement();
            stmt.executeUpdate(sql);
            stmt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public ResultSet executeQuery(String sql)  {
        try {
            log.debug("query {}", sql);
            Statement stmt = c.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet resultSet = ((PreparedStatement) stmt).executeQuery();
            return resultSet;
        } catch (SQLException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    @Override
    public void close() {
        try {
            if (c != null) {
                c.close();
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
