package org.example.hzmapstoreprototype;

import org.h2.tools.Server;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {
    public static void main(String[] args){

        try {
            var port = 3306;
            Server.createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", String.valueOf(port)).start();

            var jdbcUrl = "jdbc:h2:mem:hzpersistence;MODE=MySQL;DB_CLOSE_DELAY=-1;SCHEMA=PUBLIC;TRACE_LEVEL_SYSTEM_OUT=3";
            var username = "hazman";
            var password = "super-secret";

            var connection = DriverManager.getConnection(jdbcUrl, username, password);

            var createTableStatement = connection.createStatement();

            var createTableQuery = "CREATE TABLE htp_test (id INT PRIMARY KEY, name VARCHAR(512))";
            createTableStatement.execute(createTableQuery);

            System.out.printf("successfully started in-memory h2 instance -- listening on port %d\n", port);

            var insertEntryQuery = "INSERT INTO htp_test (id, name) VALUES (?, ?)";

            var insertEntryStatement = connection.prepareStatement(insertEntryQuery);
            insertEntryStatement.setInt(1, 4711);
            insertEntryStatement.setString(2, "awesome-value");

            var affectedRows = insertEntryStatement.executeUpdate();
            connection.close();

            System.out.printf("successfully inserted %d tuples into table\n", affectedRows);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
}