package ru.gralph2k.de;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DbHelper {
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

    public void execSql(String sql) throws SQLException {
        Statement stmt = c.createStatement();
        log.info("execute {}",sql);
        stmt.executeUpdate(sql);
        stmt.close();
    }


    public void close() throws SQLException {
        if (c!=null) {
            c.close();
        }
    }
}
