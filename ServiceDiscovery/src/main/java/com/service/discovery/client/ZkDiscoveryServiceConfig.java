package com.service.discovery.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.service.discovery.core.ServiceDiscoveryManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ComponentScan("com.service.discovery")
public class ZkDiscoveryServiceConfig {
    @Bean
    public ServiceDiscoveryManager registerServiceDiscovery(ObjectMapper objectMapper, ServiceDiscoveryClientConfig serviceDiscoveryClientConfig){
        ServiceDiscoveryManager serviceDiscoveryManager = new ServiceDiscoveryManager(objectMapper, serviceDiscoveryClientConfig);
        return serviceDiscoveryManager;
    }

    @Bean
    @Primary
    public ObjectMapper objectMapper(){
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return objectMapper;
    }
}
