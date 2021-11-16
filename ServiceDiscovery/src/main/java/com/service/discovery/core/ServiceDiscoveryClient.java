package com.service.discovery.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.service.discovery.client.ServiceDiscoveryClientConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.service.discovery.core.Constants.*;

@Slf4j
public class ServiceDiscoveryClient implements Watcher {
    private final  ObjectMapper  objectMapper;
    private ZooKeeper zooKeeper;
    private ServiceDiscoveryClientConfig serviceDiscoveryClientConfig;
    private Node node;
    private ConcurrentHashMap<String, Set<Node>> cachedNodes;

    @Autowired
    public ServiceDiscoveryClient(ObjectMapper objectMapper, ServiceDiscoveryClientConfig serviceDiscoveryClientConfig, ConcurrentHashMap<String, Set<Node>> cachedNodes) {
        this.objectMapper = objectMapper;
        this.cachedNodes=cachedNodes;
        this.serviceDiscoveryClientConfig = serviceDiscoveryClientConfig;
    }

    public  void start(){
        this.connectToZk();
        this.registerNodeToServiceDiscovery();
    }

    public  void reStart(){
        this.reconnectZk();
        this.cachedNodes.clear();
        this.registerNodeToServiceDiscovery();
    }

    public void stop(){
        log.info(LOG_PREFIX+"stopping discovery client ... ");
        this.cachedNodes.clear();
        try {
            this.zooKeeper.close();
        } catch (InterruptedException e) {
            log.error(LOG_PREFIX+"error while closing zk connection "+e);
        }
    }

    public ZooKeeper.States getZkConnectionStatus(){
        return this.zooKeeper.getState();
    }

    private void reconnectZk(){
        try {
            this.zooKeeper.close();
            connectToZk();
            while(!this.zooKeeper.getState().isConnected()){
                log.error(LOG_PREFIX+"waiting while re-connecting to zk ");
            }
        } catch (InterruptedException e) {
            log.error(LOG_PREFIX+"error while re-connecting to zk "+e);
        }
    }

    private void connectToZk(){
        try {
            Optional<ZKClientConfig> zkClientConfig = this.serviceDiscoveryClientConfig.getZKClientConfig();
            if(!zkClientConfig.isPresent()){
                this.zooKeeper = new ZooKeeper(this.serviceDiscoveryClientConfig.getZkConnectionString(),
                        this.serviceDiscoveryClientConfig.getZkConnectionTimeout(), this);
            }else{
                this.zooKeeper = new ZooKeeper(this.serviceDiscoveryClientConfig.getZkConnectionString(),
                        this.serviceDiscoveryClientConfig.getZkConnectionTimeout(), this,zkClientConfig.get());
            }
        } catch (IOException e) {
            log.error(LOG_PREFIX+"error while connecting to zk "+e);
        }
    }

    private void registerNodeToServiceDiscovery() {
        this.checkIfRootNodeExists();
        initialiseNode();
        checkIfAppRegistered();
        registerCurrentNode();
        cacheNodeDetails();
    }

    private void checkIfRootNodeExists(){
        this.zooKeeper.exists(DISCOVERY_ROOT_PATH, false, rootNodeExistCallback(),"check for root node exists");
    }

    private AsyncCallback.StatCallback rootNodeExistCallback() {
        return (int rc, String path, Object ctx, Stat stat)->{
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    log.info(LOG_PREFIX+"connection lost during checking for root node existence");
                    checkIfRootNodeExists();
                    break;
                case OK:
                case NODEEXISTS:
                    if(Objects.nonNull(stat)) {
                        log.info(LOG_PREFIX+"root path exists ..");
                    }
                    break;
                case NONODE:
                    log.info(LOG_PREFIX+"root node does not exists checking again ... ");
                    checkIfRootNodeExists();
                default: {
                    log.info(LOG_PREFIX+"exception during root Node exist check " + KeeperException.Code.get(rc).toString()+" retrying ...");
                }
            }
        };
    }

    private void initialiseNode()  {
        this.node=new Node();
        node.setNodeId(UUID.randomUUID().toString());
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            node.setNodeHost(localHost.getHostName());
            node.setIp(localHost.getHostAddress());
        } catch (UnknownHostException e) {
            log.error(LOG_PREFIX+"Not able to get host details ..");
            node.setNodeHost(UNKNOWN_HOST);
            node.setIp(LOCALHOST_IP);
        }
        node.setServiceName(serviceDiscoveryClientConfig.getServiceName());
        node.setPort(serviceDiscoveryClientConfig.getPort());
        node.setType(PROTOCOL_TYPE);
    }

    private void checkIfAppRegistered(){
        this.zooKeeper.exists(DISCOVERY_ROOT_PATH+ PATH_SLASH+node.getServiceName(), false, appRegisteredCallback(),"check for app already registered ");
    }

    private AsyncCallback.StatCallback appRegisteredCallback() {
        return (int rc, String path, Object ctx, Stat stat)->{
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    log.info(LOG_PREFIX+"connection lost during checking for app registered  ...");
                    checkIfAppRegistered();
                    break;
                case OK:
                case NODEEXISTS:
                    if(Objects.nonNull(stat)) {
                        log.info(LOG_PREFIX+"app already registered ..");
                    }
                    break;
                case NONODE:
                    log.info(LOG_PREFIX+"app is not registered , registering ... ");
                    registerAppName();
                default: {
                    log.info(LOG_PREFIX+"exception during app registered check " + KeeperException.Code.get(rc).toString()+" retrying ...");
                    checkIfAppRegistered();
                }
            }
        };
    }

    private void registerAppName(){
            this.zooKeeper.create(DISCOVERY_ROOT_PATH+ PATH_SLASH+node.getServiceName(),
                    node.getServiceName().getBytes(StandardCharsets.UTF_8),ZooDefs.Ids.OPEN_ACL_UNSAFE
                    ,CreateMode.PERSISTENT,appRegistrationCallback(),node);
    }

    private AsyncCallback.Create2Callback appRegistrationCallback() {
        return (int rc, String path, Object ctx, String name, Stat stat)->{
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    log.info(LOG_PREFIX+"connection lost during app registration to discovery "+ctx);
                    registerAppName();
                    break;
                case OK:
                    log.info(LOG_PREFIX+"app registered successfully .."+ctx);
                    break;
                case NODEEXISTS:
                    log.info(LOG_PREFIX+"app already registered , previously .."+ctx);
                    break;
                default: {
                    log.info(LOG_PREFIX+"exception during app registration " + KeeperException.Code.get(rc).toString()+" , retrying..");
                    registerAppName();
                }
            }
        };
    }

    private void registerCurrentNode()  {
        try {
            this.zooKeeper.create(DISCOVERY_ROOT_PATH+ PATH_SLASH+node.getServiceName()+PATH_SLASH+node.getNodeId(),
                    objectMapper.writeValueAsBytes(node),ZooDefs.Ids.OPEN_ACL_UNSAFE
                    ,CreateMode.EPHEMERAL,nodeRegistrationCallback(),node);
        } catch (JsonProcessingException e) {
            log.error(LOG_PREFIX+"unable to parse node "+ node +" "+e);
        }
    }

    private AsyncCallback.Create2Callback nodeRegistrationCallback() {
        return (int rc, String path, Object ctx, String name, Stat stat)->{
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    log.info(LOG_PREFIX+"connection lost during node registration to discovery "+ctx);
                    registerCurrentNode();
                    break;
                case OK:
                    log.info(LOG_PREFIX+"Node created successfully .."+ctx);
                    addWatchToDiscoveryRoot();
                    break;
                case NODEEXISTS:
                    log.info(LOG_PREFIX+"Node already registered .."+ctx);
                    addWatchToDiscoveryRoot();
                    break;
                default: {
                    log.info(LOG_PREFIX+"exception during  Node registration " + KeeperException.Code.get(rc).toString()+" , retrying..");
                    registerCurrentNode();
                }
            }
        };
    }

    private void addWatchToDiscoveryRoot() {
        this.zooKeeper.addWatch(DISCOVERY_ROOT_PATH,this,AddWatchMode.PERSISTENT_RECURSIVE
                , rootWatchAdditionCallback(),"adding watch to discovery" );
    }

    private AsyncCallback.VoidCallback rootWatchAdditionCallback() {
        return (int rc, String path, Object ctx)-> {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    log.info(LOG_PREFIX+"connection lost during root path watch creation");
                    addWatchToDiscoveryRoot();
                    break;
                case OK:
                    log.info(LOG_PREFIX+"root path watch created");
                    break;
                default: {
                    log.info(LOG_PREFIX+"exception during root Node watch creation " + KeeperException.Code.get(rc).toString()+" retrying ...");
                    addWatchToDiscoveryRoot();
                }
            }
        };
    }

    private void cacheNodeDetails(){
        this.zooKeeper.getChildren(DISCOVERY_ROOT_PATH,false,serviceDiscoveryRootChildCallback(),"getting children for root Node ..");
    }

    private AsyncCallback.Children2Callback serviceDiscoveryRootChildCallback() {
        return (int rc, String path, Object ctx, List<String> children, Stat stat)->{
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    log.info(LOG_PREFIX+"connection lost during root path child listing .. ");
                    cacheNodeDetails();
                    break;
                case OK:
                    log.info(LOG_PREFIX+"root path child fetched "+children);
                    cacheAppNodes(path,children);
                    break;
                default: {
                    log.info(LOG_PREFIX+"exception during root Node child fetch " + KeeperException.Code.get(rc).toString()+" retrying ...");
                    cacheNodeDetails();
                }
            }
        };
    }

    private void cacheAppNodes(String rootPath,List<String> children) {
        children.forEach(appRootName->{
            boolean retryLoop=true;
            int retryCount=this.serviceDiscoveryClientConfig.getZkConnectRetry();
            while(retryLoop && retryCount>0) {
                try {
                    List<String> appNodes = this.zooKeeper.getChildren(rootPath + PATH_SLASH + appRootName, false);
                    appNodes.forEach(appNodeId->cacheAppNodeData(rootPath + PATH_SLASH + appRootName,appRootName,appNodeId));
                    retryLoop=false;
                } catch (Exception e) {
                    log.error(LOG_PREFIX+"error while fetching data for app : "+rootPath + PATH_SLASH + appRootName+" retrying ...");
                    retryCount--;
                }
            }
        });
    }

    private void cacheAppNodeData(String rootPath,String appName,String appNodeId){
        boolean retryLoop=true;
        int retryCount=this.serviceDiscoveryClientConfig.getZkConnectRetry();
        while(retryLoop && retryCount>0) {
            try {
                Stat stat = new Stat();
                byte[] data = this.zooKeeper.getData(rootPath + PATH_SLASH + appNodeId, false, stat);
                Node node = objectMapper.readValue(data, Node.class);
                cachedNodes.merge(appName, Sets.newHashSet(node), (x, y) -> {
                    if (Objects.isNull(x))
                        return y;
                    x.addAll(y);
                    return x;
                });
                retryLoop = false;
            } catch (Exception e) {
                log.error(LOG_PREFIX+"error while fetching data for appNode : " + rootPath + PATH_SLASH + appNodeId + " retrying ...");
                retryCount--;
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
            case NodeDeleted:
                log.info(LOG_PREFIX+"received event of type "+event);
                processNodeDelete(event);
                break;
            case NodeChildrenChanged:
            case NodeCreated:
            case NodeDataChanged:
                    log.info(LOG_PREFIX+"received event of type "+event);
                    processNodeChange(event);
                break;
            default:
                log.info(LOG_PREFIX+"received watcher event of type "+ event);
        }
    }

    private void processNodeDelete(WatchedEvent event) {
        String path = event.getPath();
        String[] paths = path.split(PATH_SLASH);
        if(paths.length ==4){
            Optional<Node> node = this.cachedNodes.get(paths[2])
                    .stream()
                    .filter(x -> x.getNodeId().equals(paths[3]))
                    .findFirst();
            this.cachedNodes.get(paths[2]).remove(node.get());
            log.info(LOG_PREFIX+"removed node : "+node.get());
        }else{
            log.info(LOG_PREFIX+"not processing this event as node details have node changed .. "+event);
        }
    }

    private void processNodeChange(WatchedEvent event) {
        String path = event.getPath();
        String[] paths = path.split(PATH_SLASH);
        if(paths.length ==4){
            cacheAppNodeData(PATH_SLASH+paths[1]+PATH_SLASH+paths[2], paths[2],paths[3]);
        }else{
            log.info(LOG_PREFIX+"not processing this event as node details have node changed .. "+event);
        }
    }
}
