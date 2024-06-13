package org.example.hzmapstoreprototype;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Server {
    public static void main(String[] args) throws InterruptedException {

        var dataConnectionName = "hzpersistence";
        var mapName = "htp_test-0";
        var mapStoreConfig = assembleMapStoreConfig(dataConnectionName);

        var numHazelcastInstances = 2;
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
                .setProperty("jdbcUrl", "jdbc:mysql://localhost:30306/hzpersistence")
                .setProperty("user", "hazman")
                .setProperty("password", "also-super-secret")
                .setShared(false);

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