package org.example.hzmapstoreprototype;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.sql.*;

public class Server {
    public static void main(String[] args) throws InterruptedException, SQLException {

        var jdbcUrl = "jdbc:h2:tcp://localhost:3306/mem:hzpersistence;MODE=MySQL;SCHEMA=PUBLIC";
        var username = "hazman";
        var password = "super-secret";

        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM htp_test");

        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            System.out.println("ID: " + id + ", Name: " + name);
        }

        resultSet.close();
        statement.close();
        connection.close();

        var dataConnectionName = "hzpersistence";
        var mapName = "htp_test";
        var mapStoreConfig = assembleMapStoreConfig(dataConnectionName);

        var numHazelcastInstances = 1;
        HazelcastInstance[] hzInstances = new HazelcastInstance[numHazelcastInstances];
        var basePort = 5701;
        for (int i = 0; i < numHazelcastInstances; i++) {
            // Local instances will form distinct clusters, so n(instances) = n(clusters)
            hzInstances[i] = startNewHazelcastInstance("hazelcastplatform-" + i, dataConnectionName, mapName, mapStoreConfig, basePort + i);
        }

        while (true) {
            for (var hzInstance: hzInstances) {
                var m = hzInstance.getMap(mapName);
                System.out.printf("'%s': map '%s' currently contains %d element(-s)\n", hzInstance.getConfig().getClusterName(), mapName, m.size());
            }
            System.out.println();
            Thread.sleep(2000);
        }

    }

    private static HazelcastInstance startNewHazelcastInstance(String clusterName, String dataConnectionName, String mapName, MapStoreConfig mapStoreConfig, int port) {

        var hzConfig = assembleHazelcastConfig(clusterName, port);
        hzConfig.addDataConnectionConfig(assembleDataConnectionConfig(dataConnectionName));

        hzConfig.addMapConfig(assembleMapConfig(mapName, mapStoreConfig));

        return Hazelcast.newHazelcastInstance(hzConfig);

    }

    private static MapConfig assembleMapConfig(String mapName, MapStoreConfig mapStoreConfig) {

        return new MapConfig(mapName)
                .setMapStoreConfig(mapStoreConfig);

    }

    private static MapStoreConfig assembleMapStoreConfig(String dataConnectionName) {

        return new MapStoreConfig()
                .setClassName("com.hazelcast.mapstore.GenericMapStore")
                .setProperty("data-connection-ref", dataConnectionName);

    }

    private static DataConnectionConfig assembleDataConnectionConfig(String name) {

        return new DataConnectionConfig(name)
                .setType("JDBC")
                .setProperty("jdbcUrl", "jdbc:h2:tcp://localhost:3306/mem:hzpersistence;MODE=MySQL;SCHEMA=PUBLIC")
                .setProperty("user", "hazman")
                .setProperty("password", "super-secret")
                .setProperty("dialect", "H2")
                .setShared(true);

    }

    private static Config assembleHazelcastConfig(String clusterName, int port) {

        var hzConfig = new Config();
        hzConfig.setClusterName(clusterName);

        hzConfig.getMetricsConfig().setEnabled(false);
        hzConfig.getJetConfig().setEnabled(true);
        hzConfig.getNetworkConfig().setPort(port);

        return hzConfig;

    }
}