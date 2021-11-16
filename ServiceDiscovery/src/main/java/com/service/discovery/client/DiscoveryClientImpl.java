package com.service.discovery.client;

import com.google.common.collect.Sets;
import com.service.discovery.core.Node;
import com.service.discovery.core.ServiceDiscoveryManager;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;

@Component
public class DiscoveryClientImpl implements DiscoveryClient{
    private ServiceDiscoveryManager serviceDiscoveryManager;

    public DiscoveryClientImpl(ServiceDiscoveryManager serviceDiscoveryManager) {
        this.serviceDiscoveryManager = serviceDiscoveryManager;
    }

    @Override
    public Set<Node> getInstances(String serviceId) {
        return serviceDiscoveryManager.getCachedNodes().get(serviceId);
    }

    @Override
    public Set<String> getServices() {
        HashSet<String> services = Sets.newHashSet();
        for (String s : serviceDiscoveryManager.getCachedNodes().keySet()) {
            services.add(s);
        }
        return services;
    }
}
