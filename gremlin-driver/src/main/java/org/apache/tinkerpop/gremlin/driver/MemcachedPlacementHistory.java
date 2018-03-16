package org.apache.tinkerpop.gremlin.driver;

/**
 * Created by apacaci on 3/16/18.
 */
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

/**
 * Copy of JanusGraph IDPlacementHistory
 * Used By LocationAwareLoadBalancingStrategy to intelligently route traversal based on starting vertex
 * @param <T>
 */
public class MemcachedPlacementHistory<T> {

    private MemCachedClient client;

    public MemcachedPlacementHistory(String instanceName, String... servers) {
        SockIOPool pool = SockIOPool.getInstance(instanceName);
        pool.setServers(servers);
        pool.setFailover(true);
        pool.setInitConn(10);
        pool.setMinConn(5);
        pool.setMaxConn(250);
        pool.setMaintSleep(30);
        pool.setNagle(false);
        pool.setSocketTO(3000);
        pool.setAliveCheck(true);
        pool.initialize();

        client = new MemCachedClient(instanceName);
        // client.flushAll();
    }

    public Integer getPartition(T id) {
        Object value = client.get(id.toString());
        if (value == null)
            return null;
        return (Integer) value;
    }
}

