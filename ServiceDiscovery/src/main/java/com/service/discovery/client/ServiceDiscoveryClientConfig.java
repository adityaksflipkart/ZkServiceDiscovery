package com.service.discovery.client;

import lombok.Data;
import org.apache.zookeeper.client.ZKClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

@Data
@Component
public class ServiceDiscoveryClientConfig {
    @Value( "${discovery.zk.hosts}" )
    private String zkConnectionString;
    @Value( "${discovery.zk.connection.timeout:40000}" )
    private Integer zkConnectionTimeout;
    @Value( "${discovery.service.name}" )
    private String serviceName;
    @Value( "${discovery.fetchOnInit:true}" )
    private Boolean fetchOnInit;
    @Value( "${discovery.service.port}" )
    private String port;
    @Value("classpath:zookeeper.properties")
    Resource zkConfig;

    public Optional<ZKClientConfig> getZKClientConfig() throws IOException {
        if(zkConfig.exists()) {
            ZKClientConfig zkClientConfig = new ZKClientConfig();
            Properties p = new Properties();
            p.load(zkConfig.getInputStream());
            p.keySet().stream().forEach(x -> zkClientConfig.setProperty((String) x, (String) p.get(x)));
            return Optional.of(zkClientConfig);
        }
        return Optional.empty();
    }
}
