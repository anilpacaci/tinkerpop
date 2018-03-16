package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by apacaci on 3/16/18.
 */
public class LocationAwareLoadBalancingStrategy implements LoadBalancingStrategy {
    private final CopyOnWriteArrayList<Host> availableHosts = new CopyOnWriteArrayList<>();

    private final String lookupProperty;
    private final MemcachedPlacementHistory<String> placementHistory;


    public LocationAwareLoadBalancingStrategy(final String lookupProperty, final MemcachedPlacementHistory<String> placementHistory) {
        this.lookupProperty = lookupProperty;
        this.placementHistory = placementHistory;
    }

    @Override
    public void initialize(Cluster cluster, Collection<Host> hosts) {
        this.availableHosts.addAll(hosts);
    }

    @Override
    public Iterator<Host> select(RequestMessage msg) {
        return null;
    }

    @Override
    public void onAvailable(final Host host) {
        this.availableHosts.addIfAbsent(host);
    }

    @Override
    public void onUnavailable(final Host host) {
        this.availableHosts.remove(host);
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
