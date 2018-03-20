/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

