package org.example.hzmapstoreprototype;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;


public class Client {

    private static enum InteractionMode {
        INSERT,
        DELETE,
        READ
    }

    public static void main(String[] args) throws InterruptedException {

        spawnClientOnCluster("hazelcastplatform-0", "localhost:5701", InteractionMode.INSERT);

        Thread.sleep(2000);

        spawnClientOnCluster("hazelcastplatform-1", "localhost:5702", InteractionMode.READ);

    }

    private static void spawnClientOnCluster(String targetClusterName, String targetClusterAddress, InteractionMode m) throws InterruptedException {

        var clientConfig = new ClientConfig();
        clientConfig.setClusterName(targetClusterName);

        var clientNetworkConfig = clientConfig.getNetworkConfig();
        clientNetworkConfig.addAddress(targetClusterAddress);
        clientNetworkConfig.setSmartRouting(false);

        var client = HazelcastClient.newHazelcastClient(clientConfig);

        Thread.sleep(1000);

        var mapName = "htp_test-0";
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