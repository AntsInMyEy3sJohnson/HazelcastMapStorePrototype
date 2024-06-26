package org.example.hzmapstoreprototype;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;


public class Client {

    private enum InteractionMode {
        INSERT,
        DELETE,
        READ
    }

    public static void main(String[] args) throws InterruptedException {

        var targetCluster1Name = System.getenv("TARGET_CLUSTER_1_NAME");
        var targetCluster1Address = System.getenv("TARGET_CLUSTER_1_ADDRESS");
        spawnClientOnCluster(targetCluster1Name, targetCluster1Address, InteractionMode.INSERT);

        Thread.sleep(2000);

        var targetCluster2Name = System.getenv("TARGET_CLUSTER_2_NAME");
        var targetCluster2Address = System.getenv("TARGET_CLUSTER_2_ADDRESS");
        spawnClientOnCluster(targetCluster2Name, targetCluster2Address, InteractionMode.READ);

    }

    private static void spawnClientOnCluster(String targetClusterName, String targetClusterAddress, InteractionMode m) throws InterruptedException {

        var clientConfig = new ClientConfig();
        clientConfig.setClusterName(targetClusterName);

        var clientNetworkConfig = clientConfig.getNetworkConfig();
        clientNetworkConfig.addAddress(targetClusterAddress);
        clientNetworkConfig.setSmartRouting(false);

        var client = HazelcastClient.newHazelcastClient(clientConfig);

        Thread.sleep(1000);

        var mapName = "htp_test";
        var persistenceEnabledMap = client.getMap(mapName);

        for (int i = 0; i < 10; i++) {
            switch (m){
                case INSERT -> {
                    var genericRecord = GenericRecordBuilder.compact("htp")
                            .setString("value", "awesome-value-" + i)
                            .build();
                    persistenceEnabledMap.put(i, genericRecord);
                    System.out.printf("put value into map '%s' for key '%d': %s\n", mapName, i, genericRecord);
                }
                case DELETE -> persistenceEnabledMap.delete(i);
                case READ -> {
                    var valueFromMap = persistenceEnabledMap.get(i);
                    System.out.printf("read value from map '%s': %s\n", mapName, valueFromMap);
                }
            }
            Thread.sleep(100);
        }

    }

}