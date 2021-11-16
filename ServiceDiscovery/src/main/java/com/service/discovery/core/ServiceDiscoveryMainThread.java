package com.service.discovery.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.service.discovery.client.ServiceDiscoveryClientConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.service.discovery.core.Constants.LOG_PREFIX;
import static com.service.discovery.core.Constants.MAIN_THREAD_NAME;

@Slf4j
public class ServiceDiscoveryMainThread extends Thread{
    private ServiceDiscoveryClient discoveryClient;
    private ObjectMapper objectMapper;
    private ServiceDiscoveryClientConfig serviceDiscoveryClientConfig;
    private ConcurrentHashMap<String, Set<Node>> cachedNodes;
    private State state;

    public ServiceDiscoveryMainThread(String name) {
        super(name);
    }

    public static ServiceDiscoveryMainThread initialise(ServiceDiscoveryClientConfig serviceDiscoveryClientConfig, ConcurrentHashMap<String, Set<Node>> cachedNodes , ObjectMapper objectMapper){
        ServiceDiscoveryMainThread serviceDiscoveryMainThread = new ServiceDiscoveryMainThread(MAIN_THREAD_NAME);
        serviceDiscoveryMainThread.objectMapper=objectMapper;
        serviceDiscoveryMainThread.serviceDiscoveryClientConfig = serviceDiscoveryClientConfig;
        serviceDiscoveryMainThread.cachedNodes=cachedNodes;
        serviceDiscoveryMainThread.state=State.CREATED;
        return serviceDiscoveryMainThread;
    }

    @Override
    public synchronized void run() {
        if(this.state.isInit()){
            this.discoveryClient = new ServiceDiscoveryClient(objectMapper, serviceDiscoveryClientConfig,cachedNodes);
            if(this.state.isValidTransition(State.RUNNING)){
                this.state=State.RUNNING;
                this.discoveryClient.start();
            }
        }
        while (this.state.isRunning()){
            if(!this.discoveryClient.getZkConnectionStatus().isConnected()){
                this.discoveryClient.reStart();
            }
        }
        if(this.state.inClosedState()){
            if(this.state.isValidTransition(State.CLOSE_WAIT)){
                this.state=State.CLOSED;
                this.discoveryClient.stop();
            }
        }
    }

    public void stopThread(){
        log.info(LOG_PREFIX+"stopping thread on ServiceDiscoveryMainThread on shutdown ");
        this.state=State.CLOSE_WAIT;
    }

    public  enum  State{
        CREATED(new Integer[]{1,2}),RUNNING(new Integer[]{2}),CLOSE_WAIT(new Integer[]{3}),CLOSED ;
        private final Set<Integer> validTransitions = new HashSet();
        private State(Integer... validTransitions) {
            this.validTransitions.addAll(Arrays.asList(validTransitions));
        }

        public boolean isRunning() {
            return equals(RUNNING);
        }

        public boolean isInit() {
            return equals(CREATED);
        }

        public boolean inClosedState() {
            return equals(CLOSE_WAIT) || equals(CLOSED);
        }

        public boolean isValidTransition(State newState) {
            final State tmpState =  newState;
            return validTransitions.contains(tmpState.ordinal());
        }

    }

}
