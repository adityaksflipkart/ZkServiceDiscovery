package com.service.discovery.client;

import com.service.discovery.core.Node;

import java.util.Set;

public interface DiscoveryClient {
    Set<Node> getInstances(String serviceId);
    Set<String> getServices();
}
