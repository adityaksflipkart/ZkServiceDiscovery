package com.service.discovery.client;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.annotation.Order;
import org.springframework.core.type.AnnotationMetadata;

@Order(100)
public class EnableDiscoveryClientImportSelector implements ImportSelector {
    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        return new String[]{ZkDiscoveryServiceConfig.class.getName()};
    }
}
