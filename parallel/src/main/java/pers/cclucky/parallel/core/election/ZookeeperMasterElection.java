package pers.cclucky.parallel.core.election;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;

/**
 * 基于ZooKeeper的Master选举实现
 * 使用ZooKeeper临时序列节点实现分布式选举
 */
public class ZookeeperMasterElection implements MasterElectionService {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMasterElection.class);
    
    private static final String ELECTION_ROOT_PATH = "/parallel/master/election";
    private static final String ELECTION_NODE_PREFIX = "node-";
    
    private final String zkConnectString;
    private final int sessionTimeout;
    private final int connectionTimeout;
    private final String nodeId;
    
    private CuratorFramework client;
    private String myNodePath;
    private String myNodeName;
    private PathChildrenCache pathChildrenCache;
    private final AtomicBoolean isMaster = new AtomicBoolean(false);
    private String currentMasterId;
    
    private final List<MasterChangeListener> listeners = new CopyOnWriteArrayList<>();
    
    /**
     * 创建ZooKeeper选举服务
     * @param zkConnectString ZooKeeper连接字符串
     * @param nodeId 本节点ID
     */
    public ZookeeperMasterElection(String zkConnectString, String nodeId) {
        this(zkConnectString, nodeId, 5000, 3000);
    }
    
    /**
     * 创建ZooKeeper选举服务
     * @param zkConnectString ZooKeeper连接字符串
     * @param nodeId 本节点ID
     * @param sessionTimeout 会话超时时间
     * @param connectionTimeout 连接超时时间
     */
    public ZookeeperMasterElection(String zkConnectString, String nodeId, int sessionTimeout, int connectionTimeout) {
        this.zkConnectString = zkConnectString;
        this.nodeId = nodeId;
        this.sessionTimeout = sessionTimeout;
        this.connectionTimeout = connectionTimeout;
    }
    
    @Override
    public void start() {
        try {
            // 创建ZooKeeper客户端
            client = CuratorFrameworkFactory.builder()
                    .connectString(zkConnectString)
                    .sessionTimeoutMs(sessionTimeout)
                    .connectionTimeoutMs(connectionTimeout)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
            client.start();
            
            // 确保选举根节点存在
            try {
                if (client.checkExists().forPath(ELECTION_ROOT_PATH) == null) {
                    client.create()
                            .creatingParentsIfNeeded()
                            .forPath(ELECTION_ROOT_PATH);
                }
            } catch (KeeperException.NodeExistsException e) {
                // 忽略节点已存在异常
            }
            
            // 创建临时序列节点
            myNodePath = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(ELECTION_ROOT_PATH + "/" + ELECTION_NODE_PREFIX, nodeId.getBytes());
            
            myNodeName = myNodePath.substring(myNodePath.lastIndexOf('/') + 1);
            logger.info("节点 {} 创建临时节点: {}", nodeId, myNodeName);
            
            // 初始化子节点监听
            initChildrenCache();
            
            // 触发选举
            electLeader();
            
        } catch (Exception e) {
            logger.error("启动Master选举服务失败", e);
            throw new RuntimeException("启动Master选举服务失败", e);
        }
    }
    
    @Override
    public void stop() {
        try {
            if (isMaster.get()) {
                notifyLoseMaster();
            }
            
            if (pathChildrenCache != null) {
                pathChildrenCache.close();
            }
            
            if (client != null) {
                if (myNodePath != null) {
                    try {
                        client.delete().forPath(myNodePath);
                    } catch (Exception e) {
                        logger.warn("删除临时节点失败: {}", myNodePath, e);
                    }
                }
                client.close();
            }
            
            isMaster.set(false);
            currentMasterId = null;
            
        } catch (Exception e) {
            logger.error("停止Master选举服务失败", e);
        }
    }
    
    @Override
    public boolean isMaster() {
        return isMaster.get();
    }
    
    @Override
    public String getCurrentMaster() {
        return currentMasterId;
    }
    
    @Override
    public void registerMasterChangeListener(MasterChangeListener listener) {
        if (listener != null && !listeners.contains(listener)) {
            listeners.add(listener);
        }
    }
    
    @Override
    public void removeMasterChangeListener(MasterChangeListener listener) {
        listeners.remove(listener);
    }
    
    @Override
    public boolean releaseMaster() {
        if (!isMaster.get() || myNodePath == null) {
            return false;
        }
        
        try {
            client.delete().forPath(myNodePath);
            myNodePath = null;
            
            // 重新创建临时节点
            myNodePath = client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(ELECTION_ROOT_PATH + "/" + ELECTION_NODE_PREFIX, nodeId.getBytes());
            
            myNodeName = myNodePath.substring(myNodePath.lastIndexOf('/') + 1);
            logger.info("节点 {} 主动释放Master并创建新临时节点: {}", nodeId, myNodeName);
            
            notifyLoseMaster();
            electLeader();
            return true;
            
        } catch (Exception e) {
            logger.error("释放Master角色失败", e);
            return false;
        }
    }
    
    /**
     * 初始化子节点监听
     */
    private void initChildrenCache() throws Exception {
        pathChildrenCache = new PathChildrenCache(client, ELECTION_ROOT_PATH, true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
                switch (event.getType()) {
                    case CHILD_ADDED:
                    case CHILD_REMOVED:
                        try {
                            electLeader();
                        } catch (Exception e) {
                            logger.error("选举处理失败", e);
                        }
                        break;
                    default:
                        break;
                }
            }
        });
        pathChildrenCache.start();
    }
    
    /**
     * 执行Leader选举
     */
    private void electLeader() throws Exception {
        List<String> children = client.getChildren().forPath(ELECTION_ROOT_PATH);
        Collections.sort(children);
        
        String leaderNode = children.get(0);
        String oldMasterId = currentMasterId;
        
        // 读取当前Master节点的数据
        String masterId = null;
        if (!children.isEmpty()) {
            try {
                byte[] masterData = client.getData().forPath(ELECTION_ROOT_PATH + "/" + leaderNode);
                masterId = new String(masterData);
            } catch (Exception e) {
                logger.warn("读取Master节点数据失败", e);
            }
        }
        
        boolean wasLeader = isMaster.get();
        boolean isLeaderNow = myNodeName.equals(leaderNode);
        
        isMaster.set(isLeaderNow);
        currentMasterId = masterId;
        
        if (wasLeader && !isLeaderNow) {
            // 失去Leader地位
            logger.info("节点 {} 失去Master角色", nodeId);
            notifyLoseMaster();
        } else if (!wasLeader && isLeaderNow) {
            // 成为Leader
            logger.info("节点 {} 成为Master", nodeId);
            notifyBecomeMaster();
        }
        
        if (!Objects.equals(oldMasterId, currentMasterId)) {
            // Master发生变更
            notifyMasterChanged(oldMasterId, currentMasterId);
        }
        
        if (!isLeaderNow) {
            // 监听前一个节点
            setupWatchOnPreviousNode(children);
        }
    }
    
    /**
     * 监听前一个节点
     * @param allNodes 所有节点列表
     */
    private void setupWatchOnPreviousNode(List<String> allNodes) {
        if (allNodes.isEmpty() || myNodeName == null) {
            return;
        }
        
        String previousNode = null;
        int myIndex = allNodes.indexOf(myNodeName);
        
        if (myIndex > 0) {
            previousNode = allNodes.get(myIndex - 1);
        }
        
        if (previousNode != null) {
            try {
                // 监听前一个节点
                String previousNodePath = ELECTION_ROOT_PATH + "/" + previousNode;
                client.checkExists().usingWatcher(new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        // 前一个节点发生变更，重新选举
                        try {
                            electLeader();
                        } catch (Exception e) {
                            logger.error("选举Leader失败", e);
                        }
                    }
                }).forPath(previousNodePath);
                
                logger.debug("节点 {} 设置对前一个节点 {} 的监听", nodeId, previousNode);
            } catch (Exception e) {
                logger.warn("设置对前一个节点的监听失败", e);
            }
        }
    }
    
    /**
     * 通知成为Master
     */
    private void notifyBecomeMaster() {
        for (MasterChangeListener listener : listeners) {
            try {
                listener.onBecomeMaster(nodeId);
            } catch (Exception e) {
                logger.error("通知成为Master事件失败", e);
            }
        }
    }
    
    /**
     * 通知失去Master
     */
    private void notifyLoseMaster() {
        for (MasterChangeListener listener : listeners) {
            try {
                listener.onLoseMaster(nodeId);
            } catch (Exception e) {
                logger.error("通知失去Master事件失败", e);
            }
        }
    }
    
    /**
     * 通知Master变更
     */
    private void notifyMasterChanged(String oldMasterId, String newMasterId) {
        for (MasterChangeListener listener : listeners) {
            try {
                listener.onMasterChange(oldMasterId, newMasterId);
            } catch (Exception e) {
                logger.error("通知Master变更事件失败", e);
            }
        }
    }
} 