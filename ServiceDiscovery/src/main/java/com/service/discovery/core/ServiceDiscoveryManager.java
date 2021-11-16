package com.service.discovery.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.service.discovery.client.ServiceDiscoveryClientConfig;
import org.springframework.beans.factory.DisposableBean;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceDiscoveryManager implements DisposableBean {
    private final ConcurrentHashMap<String, Set<Node>> cachedNodes;
    private ServiceDiscoveryMainThread serviceDiscoveryMainThread;

    public ServiceDiscoveryManager(ObjectMapper objectMapper, ServiceDiscoveryClientConfig serviceDiscoveryClientConfig) {
        this.cachedNodes = new ConcurrentHashMap<>();;
        this.serviceDiscoveryMainThread=ServiceDiscoveryMainThread.initialise(serviceDiscoveryClientConfig,cachedNodes,objectMapper);
        this.serviceDiscoveryMainThread.start();
    }

    @Override
    public void destroy() throws Exception {
        this.serviceDiscoveryMainThread.stopThread();
    }

    public ConcurrentHashMap<String, Set<Node>> getCachedNodes() {
        return cachedNodes;
    }
}
