package com.test;

import com.service.discovery.client.DiscoveryClient;
import com.service.discovery.core.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

@RestController
@RequestMapping("/api")
public class SampleController {
    @Autowired
    private DiscoveryClient discoveryClient;

    @RequestMapping("/test/{applicationName}")
    public ResponseEntity<Set<Node>> serviceInstancesByApplicationName(@PathVariable String applicationName) {
        return ResponseEntity.ok(discoveryClient.getInstances(applicationName));
    }
}
