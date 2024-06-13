package org.example.hzmapstoreprototype;

import org.h2.tools.Server;

import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {
    public static void main(String[] args){


        try {
            var port = 3306;
            Server.createTcpServer("-tcpAllowOthers", "-tcpPort", String.valueOf(port)).start();

            var jdbcUrl = String.format("jdbc:h2:mem://localhost:%d/~/hzpersistence;MODE=MySQL", port);
            var username = "hazman";
            var password = "super-secret";

            var connection = DriverManager.getConnection(jdbcUrl, username, password);

            var createTableStatement = connection.createStatement();

            var createTableQuery = "CREATE TABLE htp_test (id INT PRIMARY KEY, name VARCHAR(512))";
            createTableStatement.execute(createTableQuery);

            System.out.printf("successfully started in-memory h2 instance -- listening on port %d\n", port);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
}