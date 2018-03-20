package org.apache.tinkerpop.gremlin.driver;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by apacaci on 3/19/18.
 */
public class LocationAwareLoadBalancingStrategyTest {

    private static Cluster cluster;
    private static Client remoteClient;

    @BeforeClass
    public static void initializeRemoteDriver() throws Exception {
        cluster = Cluster.open("src/test/resources/remote-objects.yaml");
        remoteClient = cluster.connect();
    }

    @Test
    public void dummyTestQuery() throws ExecutionException, InterruptedException {
        Map<String, Object> params = new HashMap<>();
        params.put("person_id", "person:933");
        remoteClient.submit("g.V().has(\"iid\", person_id))", params).all().get();
        remoteClient.submit("g.V().has(\"iid\", person_id))", params).all().get();
    }

    @AfterClass
    public static void closeConnection() {
        remoteClient.close();
        cluster.close();
    }
}
