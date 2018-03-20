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
