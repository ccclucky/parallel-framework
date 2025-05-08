package pers.cclucky.parallel.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.core.election.MasterChangeListener;
import pers.cclucky.parallel.core.election.MasterElectionService;
import pers.cclucky.parallel.core.election.ZookeeperMasterElection;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 简化版主节点
 * 仅实现Master选举功能，保持与ParallelFramework相同的接口
 * 用于测试时替代ParallelFramework
 */
public class SimpleMasterNode {
    private static final Logger logger = LoggerFactory.getLogger(SimpleMasterNode.class);
    
    // 节点ID
    private final String nodeId;
    // 配置属性
    private final Properties config;
    // Master选举服务
    private MasterElectionService electionService;
    // 节点状态
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    // 服务实例映射
    private final Map<String, Object> services = new ConcurrentHashMap<>();
    
    /**
     * 创建简化版主节点
     */
    public SimpleMasterNode() {
        this.nodeId = "node-" + UUID.randomUUID().toString().substring(0, 8);
        this.config = new Properties();
        config.setProperty("zookeeper.address", "127.0.0.1:2181");
    }
    
    /**
     * 创建简化版主节点
     * @param nodeId 节点ID
     * @param zkAddress ZooKeeper地址
     */
    public SimpleMasterNode(String nodeId, String zkAddress) {
        this.nodeId = nodeId;
        this.config = new Properties();
        config.setProperty("zookeeper.address", zkAddress);
    }
    
    /**
     * 初始化节点
     * @throws Exception 如果初始化失败
     */
    public void init() throws Exception {
        if (initialized.compareAndSet(false, true)) {
            logger.info("初始化简化版主节点，节点ID: {}", nodeId);
            
            // 创建Master选举服务
            String zkAddress = config.getProperty("zookeeper.address", "127.0.0.1:2181");
            electionService = new ZookeeperMasterElection(zkAddress, nodeId);
            
            // 添加Master变更监听器
            electionService.registerMasterChangeListener(new MasterChangeListener() {
                @Override
                public void onBecomeMaster(String nodeId) {
                    logger.info("节点 {} 成为Master", nodeId);
                }
                
                @Override
                public void onLoseMaster(String nodeId) {
                    logger.info("节点 {} 失去Master角色", nodeId);
                }
                
                @Override
                public void onMasterChange(String oldMasterId, String newMasterId) {
                    logger.info("Master变更: {} -> {}", oldMasterId, newMasterId);
                }
            });
            
            registerService("MasterElectionService", electionService);
            logger.info("简化版主节点初始化完成");
        } else {
            logger.warn("节点已经初始化");
        }
    }
    
    /**
     * 启动节点
     * @throws Exception 如果启动失败
     */
    public void start() throws Exception {
        if (!initialized.get()) {
            throw new IllegalStateException("节点尚未初始化，请先调用init()方法");
        }
        
        if (running.compareAndSet(false, true)) {
            logger.info("启动简化版主节点");
            
            // 启动Master选举
            electionService.start();
            
            logger.info("简化版主节点已启动");
        } else {
            logger.warn("节点已经在运行");
        }
    }
    
    /**
     * 停止节点
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("停止简化版主节点");
            
            // 停止Master选举
            if (electionService != null) {
                electionService.stop();
            }
            
            logger.info("简化版主节点已停止");
        } else {
            logger.warn("节点未在运行");
        }
    }
    
    /**
     * 注册服务实例
     * @param serviceName 服务名称
     * @param service 服务实例
     */
    private void registerService(String serviceName, Object service) {
        services.put(serviceName, service);
    }
    
    /**
     * 获取服务实例
     * @param serviceName 服务名称
     * @param <T> 服务类型
     * @return 服务实例
     */
    @SuppressWarnings("unchecked")
    public <T> T getService(String serviceName) {
        return (T) services.get(serviceName);
    }
    
    /**
     * 获取框架是否已初始化
     * @return 是否已初始化
     */
    public boolean isInitialized() {
        return initialized.get();
    }
    
    /**
     * 获取框架是否正在运行
     * @return 是否正在运行
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * 获取当前节点ID
     * @return 节点ID
     */
    public String getNodeId() {
        return nodeId;
    }
    
    /**
     * 获取当前节点是否为Master
     * @return 是否为Master
     */
    public boolean isMaster() {
        return electionService != null && electionService.isMaster();
    }
    
    /**
     * 获取当前Master节点ID
     * @return Master节点ID
     */
    public String getCurrentMaster() {
        return electionService != null ? electionService.getCurrentMaster() : null;
    }
    
    /**
     * 获取配置属性值
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值
     */
    public String getConfigProperty(String key, String defaultValue) {
        return config.getProperty(key, defaultValue);
    }
    
    /**
     * 设置配置属性值
     * @param key 配置键
     * @param value 配置值
     */
    public void setConfigProperty(String key, String value) {
        config.setProperty(key, value);
    }
} 