package com.service.discovery.core;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class Node {
    private String nodeId;
    private String nodeHost;
    private String serviceName;
    private String ip;
    private String port;
    private String type;
    public String fetchUrl(){
        return type+"://"+ip+":"+port;
    }

}
