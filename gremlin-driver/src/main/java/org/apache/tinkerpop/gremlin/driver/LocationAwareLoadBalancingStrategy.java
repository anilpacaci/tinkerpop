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

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by apacaci on 3/16/18.
 */
public class LocationAwareLoadBalancingStrategy implements LoadBalancingStrategy {
    private final CopyOnWriteArrayList<Host> availableHosts = new CopyOnWriteArrayList<>();
    private final AtomicInteger index = new AtomicInteger();

    private final String lookupProperty;
    private final MemcachedPlacementHistory<String> placementHistory;
    private final Map<Integer, Host> partitionHostMappings;
    private final Map<String, Integer> urlPartitionMappings;


    public LocationAwareLoadBalancingStrategy(final String lookupProperty, final MemcachedPlacementHistory<String> placementHistory, final Map<String, Integer> urlPartitionMappings) {
        this.lookupProperty = lookupProperty;
        this.placementHistory = placementHistory;
        this.urlPartitionMappings = urlPartitionMappings;
        this.partitionHostMappings = new HashMap<>();
    }

    @Override
    public void initialize(Cluster cluster, Collection<Host> hosts) {
        this.availableHosts.addAll(hosts);
        this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));

        hosts.stream().forEach(host -> {
            Integer partition = urlPartitionMappings.get(host.getHostUri().getHost());
            partitionHostMappings.put(partition, host);
        });
    }

    @Override
    public Iterator<Host> select(RequestMessage msg) {
        final List<Host> hosts = new ArrayList<>();

        // get the value of lookup property from argument bindings
        Map<String, String> bindings = (Map<String, String>) msg.getArgs().get("bindings");
        Object lookupParamater = bindings.get(lookupProperty);

        if(lookupParamater != null) {
            // lookup parameter is not null, so check partition mapping to retrieve correct partition id for this parameter
            Integer partition = placementHistory.getPartition(lookupParamater.toString());
            if(partition != null) {
                // we can find partition baed on lookup property, so return that host
                Host selectedHost = partitionHostMappings.get(partition);
                hosts.add(selectedHost);
            }
        }

        // regardless of we find partition mapping, add all available hosts based on round robin logic
        final int startIndex = index.getAndIncrement();
        for(int i = 0 ; i < availableHosts.size(); i++) {
            // always add starting from index, to implement round robin logic
            hosts.add(availableHosts.get( (startIndex + i) % availableHosts.size() ));
        }

        return new Iterator<Host>() {

            private int currentIndex = 0;
            private int remainings = hosts.size();

            @Override
            public boolean hasNext() {
                return remainings > 0;
            }

            @Override
            public Host next() {
                //decrement remainings
                remainings--;
                // return the current index, then increment the pointer
                return hosts.get(currentIndex++);
            }
        };
    }

    @Override
    public void onAvailable(final Host host) {
        this.availableHosts.addIfAbsent(host);
        Integer partition = urlPartitionMappings.get(host.getHostUri().getHost());
        this.partitionHostMappings.put(partition, host);
    }

    @Override
    public void onUnavailable(final Host host) {
        this.availableHosts.remove(host);
        Integer partition = urlPartitionMappings.get(host.getHostUri().getHost());
        this.partitionHostMappings.remove(partition);
    }

    @Override
    public void onNew(final Host host) {
        onAvailable(host);
    }

    @Override
    public void onRemove(final Host host) {
        onUnavailable(host);
    }
}
