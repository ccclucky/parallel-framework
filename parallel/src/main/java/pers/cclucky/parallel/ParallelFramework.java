package pers.cclucky.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import pers.cclucky.parallel.api.service.TaskService;
import pers.cclucky.parallel.api.service.DefaultTaskService;
import pers.cclucky.parallel.core.election.MasterChangeListener;
import pers.cclucky.parallel.core.election.MasterElectionService;
import pers.cclucky.parallel.core.election.ZookeeperMasterElection;
import pers.cclucky.parallel.core.heartbeat.AdaptiveHeartbeatDetector;
import pers.cclucky.parallel.core.heartbeat.HeartbeatDetector;
import pers.cclucky.parallel.core.heartbeat.WorkerFailureListener;
import pers.cclucky.parallel.core.storage.RedisTaskStorage;
import pers.cclucky.parallel.core.storage.TaskStorage;
import pers.cclucky.parallel.core.schedule.DistributedTaskScheduler;
import pers.cclucky.parallel.core.schedule.TaskScheduler;
import pers.cclucky.parallel.core.worker.Worker;
import pers.cclucky.parallel.core.worker.WorkerManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 并行计算框架主类
 * 集成各组件，提供框架的初始化、启动和停止功能
 */
public class ParallelFramework {
    private static final Logger logger = LoggerFactory.getLogger(ParallelFramework.class);
    
    // 配置属性
    private Properties config;
    
    // 节点ID
    private String nodeId;
    
    // 组件实例
    private MasterElectionService masterElectionService;
    private TaskStorage taskStorage;
    private HeartbeatDetector heartbeatDetector;
    private RedissonClient redissonClient;
    
    // 已注册的服务
    private final Map<String, Object> services = new ConcurrentHashMap<>();
    
    // 框架状态
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    // 心跳任务执行器
    private ScheduledExecutorService heartbeatExecutor;
    
    // 新增WorkerManager实例
    private WorkerManager workerManager;
    
    /**
     * 创建并行计算框架
     */
    public ParallelFramework() {
        // 默认构造
    }
    
    /**
     * 创建并行计算框架（带配置文件）
     * @param configFile 配置文件路径
     * @throws IOException 如果配置文件读取失败
     */
    public ParallelFramework(String configFile) throws IOException {
        loadConfig(configFile);
    }
    
    /**
     * 初始化框架
     * @throws Exception 如果初始化失败
     */
    public void init() throws Exception {
        if (initialized.compareAndSet(false, true)) {
            logger.info("初始化并行计算框架");
            
            // 加载配置（如果尚未加载）
            if (config == null) {
                loadDefaultConfig();
            }
            
            // 初始化节点ID
            initNodeId();
            
            // 初始化Redis连接
            initRedis();
            
            // 初始化任务存储
            initTaskStorage();
            
            // 初始化Worker
            initWorker();
            
            // 初始化心跳检测
            initHeartbeatDetector();
            
            // 初始化Master选举
            initMasterElection();
            
            // 初始化任务调度器和服务
            initTaskScheduler();
            
            logger.info("并行计算框架初始化完成，节点ID: {}", nodeId);
        } else {
            logger.warn("框架已经初始化");
        }
    }
    
    /**
     * 启动框架
     * @throws Exception 如果启动失败
     */
    public void start() throws Exception {
        if (!initialized.get()) {
            throw new IllegalStateException("框架尚未初始化，请先调用init()方法");
        }
        
        if (running.compareAndSet(false, true)) {
            logger.info("启动并行计算框架");
            
            // 启动心跳检测器
            heartbeatDetector.start();
            
            // 注册当前节点到集群
            Map<String, Object> initialInfo = new HashMap<>();
            initialInfo.put("nodeId", nodeId);
            initialInfo.put("startTime", System.currentTimeMillis());
            initialInfo.put("isMaster", masterElectionService.isMaster());
            initialInfo.put("heartbeatTime", System.currentTimeMillis());
            
            boolean registered = heartbeatDetector.registerWorker(nodeId, initialInfo);
            if (registered) {
                logger.info("节点已成功注册到集群: {}", nodeId);
            } else {
                logger.warn("节点注册失败: {}", nodeId);
            }
            
            // 启动定时心跳任务
            startHeartbeatTask();
            
            // 启动Master选举
            masterElectionService.start();
            
            logger.info("并行计算框架已启动");
        } else {
            logger.warn("框架已经在运行");
        }
    }
    
    /**
     * 停止框架
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("停止并行计算框架");
            
            // 从集群注销当前节点
            if (heartbeatDetector != null) {
                boolean unregistered = heartbeatDetector.unregisterWorker(nodeId);
                if (unregistered) {
                    logger.info("节点已从集群注销: {}", nodeId);
                } else {
                    logger.warn("节点注销失败: {}", nodeId);
                }
            }
            
            // 停止Master选举
            if (masterElectionService != null) {
                masterElectionService.stop();
            }
            
            // 停止心跳任务
            stopHeartbeatTask();
            
            // 停止心跳检测器
            if (heartbeatDetector != null) {
                heartbeatDetector.stop();
            }
            
            // 关闭Redis客户端
            if (redissonClient != null) {
                redissonClient.shutdown();
            }
            
            // 停止Worker
            if (workerManager != null) {
                workerManager.stop();
            }
            
            logger.info("并行计算框架已停止");
        } else {
            logger.warn("框架未在运行");
        }
    }
    
    /**
     * 加载默认配置
     */
    private void loadDefaultConfig() {
        try {
            String configDir = System.getProperty("config.dir", ".");
            File configFile = new File(configDir, "application.properties");
            
            if (configFile.exists()) {
                logger.info("加载配置文件: {}", configFile.getAbsolutePath());
                loadConfig(configFile.getAbsolutePath());
            } else {
                logger.info("未找到配置文件，使用默认配置");
                config = createDefaultConfig();
            }
        } catch (Exception e) {
            logger.warn("加载默认配置失败，使用内存默认值", e);
            config = createDefaultConfig();
        }
    }
    
    /**
     * 加载配置文件
     * @param configFile 配置文件路径
     * @throws IOException 如果配置文件读取失败
     */
    private void loadConfig(String configFile) throws IOException {
        config = new Properties();
        ClassPathResource resource = new ClassPathResource(configFile);
        try (InputStream is = resource.getInputStream()) {
            config.load(is);
        }
    }
    
    /**
     * 创建默认配置
     * @return 默认配置
     */
    private Properties createDefaultConfig() {
        Properties props = new Properties();
        
        // 应用配置
        props.setProperty("app.name", "parallel-framework");
        props.setProperty("app.version", "1.0.0");
        props.setProperty("server.port", "8080");
        
        // ZooKeeper配置
        props.setProperty("zookeeper.address", "127.0.0.1:2181");
        props.setProperty("zookeeper.session.timeout", "30000");
        props.setProperty("zookeeper.connection.timeout", "10000");
        
        // Redis配置
        props.setProperty("redis.host", "127.0.0.1");
        props.setProperty("redis.port", "6379");
        props.setProperty("redis.database", "0");
        props.setProperty("redis.timeout", "10000");
        props.setProperty("redis.connection.pool.size", "8");
        props.setProperty("redis.connection.min.idle", "2");
        
        // Dubbo配置
        props.setProperty("dubbo.application.name", "parallel-framework");
        props.setProperty("dubbo.registry.address", "zookeeper://127.0.0.1:2181");
        props.setProperty("dubbo.protocol.name", "dubbo");
        props.setProperty("dubbo.protocol.port", "20880");
        
        // 心跳配置
        props.setProperty("heartbeat.base.interval", "3000");
        props.setProperty("heartbeat.base.timeout", "10000");
        
        // 任务配置
        props.setProperty("task.retry.max", "3");
        
        return props;
    }
    
    /**
     * 初始化节点ID
     */
    private void initNodeId() {
        nodeId = config.getProperty("node.id");
        if (nodeId == null || nodeId.isEmpty()) {
            nodeId = "node-" + UUID.randomUUID().toString().substring(0, 8);
            config.setProperty("node.id", nodeId);
        }
    }
    
    /**
     * 初始化Redis连接
     */
    private void initRedis() {
        String redisHost = config.getProperty("redis.host", "127.0.0.1");
        int redisPort = Integer.parseInt(config.getProperty("redis.port", "6379"));
        String redisPassword = config.getProperty("redis.password", "");
        int redisDatabase = Integer.parseInt(config.getProperty("redis.database", "0"));
        int poolSize = Integer.parseInt(config.getProperty("redis.connection.pool.size", "8"));
        int minIdle = Integer.parseInt(config.getProperty("redis.connection.min.idle", "2"));
        
        logger.info("初始化Redis连接: {}:{}/{}", redisHost, redisPort, redisDatabase);
        
        try {
            // 创建Redisson配置
            Config redissonConfig = new Config();
            
            // 配置单机模式
            redissonConfig.useSingleServer()
                .setAddress("redis://" + redisHost + ":" + redisPort)
                .setDatabase(redisDatabase)
                .setConnectionPoolSize(poolSize)
                .setConnectionMinimumIdleSize(minIdle);
            
            // 如果有密码，则设置密码
            if (redisPassword != null && !redisPassword.isEmpty()) {
                redissonConfig.useSingleServer().setPassword(redisPassword);
            }
            
            // 创建Redisson客户端
            redissonClient = Redisson.create(redissonConfig);
            
            logger.info("Redis连接初始化完成");
            
        } catch (Exception e) {
            logger.error("初始化Redis连接失败", e);
            throw new RuntimeException("初始化Redis连接失败", e);
        }
    }
    
    /**
     * 初始化任务存储
     */
    private void initTaskStorage() {
        logger.info("初始化任务存储");
        taskStorage = new RedisTaskStorage(redissonClient);
    }
    
    /**
     * 初始化Worker
     */
    private void initWorker() {
        logger.info("初始化Worker");
        workerManager = new WorkerManager(nodeId, taskStorage);
        workerManager.start();
        
        // 注册Worker服务
        Worker worker = workerManager.getWorker();
        registerService("worker", worker);
        
        logger.info("Worker初始化完成");
    }
    
    /**
     * 初始化任务调度器和服务
     */
    private void initTaskScheduler() {
        logger.info("初始化任务调度器");
        TaskScheduler taskScheduler = new DistributedTaskScheduler(
            nodeId, taskStorage, redissonClient, masterElectionService, heartbeatDetector);
        taskScheduler.start();
        
        // 注册服务
        registerService("pers.cclucky.parallel.core.schedule.TaskScheduler", taskScheduler);
        
        // 创建并注册TaskService
        TaskService taskService = new DefaultTaskService(taskScheduler);
        registerService("taskService", taskService);
        
        logger.info("任务调度器和服务初始化完成");
    }
    
    /**
     * 初始化心跳检测器
     */
    private void initHeartbeatDetector() {
        heartbeatDetector = new AdaptiveHeartbeatDetector(taskStorage);
        
        // 添加Worker节点失效监听器
        ((AdaptiveHeartbeatDetector) heartbeatDetector).addWorkerFailureListener(
                new WorkerFailureListener() {
                    @Override
                    public void onWorkerFailure(String workerId, Map<String, Object> lastHeartbeat) {
                        handleWorkerFailure(workerId, lastHeartbeat);
                    }
                }
        );
        
        registerService(HeartbeatDetector.class.getName(), heartbeatDetector);
        logger.info("心跳检测器初始化完成");
    }
    
    /**
     * 处理Worker节点失效
     * @param workerId Worker节点ID
     * @param lastHeartbeat 最后一次心跳信息
     */
    private void handleWorkerFailure(String workerId, Map<String, Object> lastHeartbeat) {
        logger.warn("Worker节点失效: {}, 最后心跳: {}", workerId, lastHeartbeat);
        
        // 如果当前节点不是Master，则无需处理
        if (!masterElectionService.isMaster()) {
            logger.info("当前节点不是Master，不处理Worker失效");
            return;
        }
        
        try {
            // 获取Worker上的任务分片
            List<Map<String, String>> workerSlices = taskStorage.getWorkerSlices(workerId);
            if (workerSlices == null || workerSlices.isEmpty()) {
                logger.info("Worker节点没有正在处理的任务分片: {}", workerId);
                return;
            }
            
            // 获取活跃的Worker列表
            Map<String, Map<String, Object>> activeWorkers = new HashMap<>();
            // 从心跳检测器获取所有活跃节点信息
            for (String nodeId : heartbeatDetector.getAliveWorkers()) {
                if (!nodeId.equals(workerId) && heartbeatDetector.isWorkerAlive(nodeId)) {
                    Map<String, Object> heartbeat = taskStorage.getWorkerHeartbeat(nodeId);
                    if (heartbeat != null) {
                        activeWorkers.put(nodeId, heartbeat);
                    }
                }
            }
            
            if (activeWorkers.isEmpty()) {
                logger.warn("没有活跃的Worker节点，无法重新分配任务");
                return;
            }
            
            // 使用一致性哈希负载均衡器
            pers.cclucky.parallel.core.loadbalance.LoadBalancer loadBalancer = 
                    new pers.cclucky.parallel.core.loadbalance.ConsistentHashLoadBalancer();
            List<String> activeWorkerIds = new ArrayList<>(activeWorkers.keySet());
            
            int reassignedCount = 0;
            for (Map<String, String> sliceInfo : workerSlices) {
                String taskId = sliceInfo.get("taskId");
                String sliceId = sliceInfo.get("sliceId");
                
                try {
                    // 选择新的Worker节点
                    String newWorkerId = loadBalancer.selectNode(activeWorkerIds, taskId + ":" + sliceId);
                    
                    // 重新分配分片
                    if (taskStorage.assignSliceToWorker(taskId, sliceId, newWorkerId)) {
                        reassignedCount++;
                        logger.info("成功重新分配任务分片: taskId={}, sliceId={}, from={}, to={}", 
                                taskId, sliceId, workerId, newWorkerId);
                    }
                } catch (Exception e) {
                    logger.error("重新分配任务分片失败: taskId={}, sliceId={}, error={}", 
                            taskId, sliceId, e.getMessage(), e);
                }
            }
            
            logger.info("Worker节点失效处理完成，共重新分配{}个任务分片", reassignedCount);
        } catch (Exception e) {
            logger.error("处理Worker节点失效出错: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 初始化Master选举
     */
    private void initMasterElection() {
        String zkAddress = config.getProperty("zookeeper.address", "127.0.0.1:2181");
        int sessionTimeout = Integer.parseInt(config.getProperty("zookeeper.session.timeout", "30000"));
        int connectionTimeout = Integer.parseInt(config.getProperty("zookeeper.connection.timeout", "10000"));
        
        masterElectionService = new ZookeeperMasterElection(zkAddress, nodeId, sessionTimeout, connectionTimeout);
        
        // 添加Master变更监听器
        masterElectionService.registerMasterChangeListener(new MasterChangeListener() {
            @Override
            public void onBecomeMaster(String nodeId) {
                handleBecomeMaster(nodeId);
            }
            
            @Override
            public void onLoseMaster(String nodeId) {
                handleLoseMaster(nodeId);
            }
            
            @Override
            public void onMasterChange(String oldMasterId, String newMasterId) {
                handleMasterChange(oldMasterId, newMasterId);
            }
        });
        
        registerService(MasterElectionService.class.getName(), masterElectionService);
        logger.info("Master选举服务初始化完成, ZooKeeper地址: {}", zkAddress);
    }
    
    /**
     * 处理成为Master事件
     * @param nodeId 节点ID
     */
    private void handleBecomeMaster(String nodeId) {
        logger.info("节点 {} 成为Master", nodeId);
        
        if (!this.nodeId.equals(nodeId)) {
            logger.warn("收到其他节点成为Master的事件通知，忽略");
            return;
        }
        
        try {
            // 检查并处理所有运行中的任务
            List<String> runningTasks = taskStorage.getRunningTasks();
            if (runningTasks != null && !runningTasks.isEmpty()) {
                logger.info("发现{}个运行中的任务需要处理", runningTasks.size());
                
                // 启动任务调度器处理未完成的任务
                Object taskScheduler = services.get("pers.cclucky.parallel.core.schedule.TaskScheduler");
                if (taskScheduler != null && taskScheduler instanceof pers.cclucky.parallel.core.schedule.TaskScheduler) {
                    pers.cclucky.parallel.core.schedule.TaskScheduler scheduler = 
                            (pers.cclucky.parallel.core.schedule.TaskScheduler) taskScheduler;
                    
                    // 对每个运行中的任务进行重新调度
                    for (String taskId : runningTasks) {
                        try {
                            logger.info("重新调度任务: {}", taskId);
                            scheduler.rescheduleTask(taskId);
                        } catch (Exception e) {
                            logger.error("重新调度任务失败: taskId={}, error={}", taskId, e.getMessage(), e);
                        }
                    }
                } else {
                    logger.warn("任务调度器未初始化或不可用，无法处理未完成任务");
                }
            }
            
            // 扫描所有Worker节点，检测心跳
            if (heartbeatDetector != null) {
                logger.info("等待心跳检测器检测节点状态");
            }
            
            logger.info("Master节点初始化完成");
        } catch (Exception e) {
            logger.error("成为Master后处理失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 处理失去Master事件
     * @param nodeId 节点ID
     */
    private void handleLoseMaster(String nodeId) {
        logger.info("节点 {} 失去Master角色", nodeId);
        
        if (!this.nodeId.equals(nodeId)) {
            logger.warn("收到其他节点失去Master的事件通知，忽略");
            return;
        }
        
        try {
            // 停止本地任务调度
            Object taskScheduler = services.get("pers.cclucky.parallel.core.schedule.TaskScheduler");
            if (taskScheduler != null && taskScheduler instanceof pers.cclucky.parallel.core.schedule.TaskScheduler) {
                // 没有完全停止调度器，而是暂停调度新任务
                logger.info("暂停任务调度");
            }
            
            logger.info("节点已退出Master角色");
            
            // 等待一段时间再重新参与选举，避免频繁切换Master
            boolean shouldReelectImmediately = Boolean.parseBoolean(
                    config.getProperty("master.reelect.immediately", "true"));
            
            if (shouldReelectImmediately) {
                logger.info("配置为立即重新参与Master选举");
                
                // 判断当前节点状态是否适合重新参与选举
                if (running.get() && initialized.get()) {
                    logger.info("节点状态正常，重新参与Master选举");
                    
                    // 调用releaseMaster重新参与选举
                    if (masterElectionService.releaseMaster()) {
                        logger.info("成功重新注册选举节点");
                    } else {
                        logger.warn("重新注册选举节点失败");
                    }
                } else {
                    logger.warn("节点状态异常，不重新参与Master选举");
                }
            } else {
                logger.info("配置为不立即重新参与Master选举，等待下一轮自动选举");
            }
            
        } catch (Exception e) {
            logger.error("失去Master角色处理失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 处理Master变更事件
     * @param oldMasterId 旧Master节点ID
     * @param newMasterId 新Master节点ID
     */
    private void handleMasterChange(String oldMasterId, String newMasterId) {
        logger.info("Master变更: {} -> {}", oldMasterId, newMasterId);
        
        try {
            // 当前节点是新Master
            if (this.nodeId.equals(newMasterId)) {
                handleBecomeMaster(newMasterId);
            } 
            // 当前节点是旧Master
            else if (this.nodeId.equals(oldMasterId)) {
                handleLoseMaster(oldMasterId);
            } 
            // 当前节点既不是新Master也不是旧Master
            else {
                logger.info("Master变更，当前节点角色不变");
                
                // 更新本地缓存的Master信息
                // 如果有需要与Master通信的服务，可以在这里更新
            }
        } catch (Exception e) {
            logger.error("处理Master变更事件失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 启动心跳任务
     */
    private void startHeartbeatTask() {
        if (heartbeatExecutor == null || heartbeatExecutor.isShutdown()) {
            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "heartbeat-sender");
                t.setDaemon(true);
                return t;
            });
            
            long heartbeatInterval = Long.parseLong(config.getProperty("heartbeat.base.interval", "3000"));
            
            heartbeatExecutor.scheduleAtFixedRate(() -> {
                try {
                    sendHeartbeat();
                } catch (Exception e) {
                    logger.error("发送心跳失败", e);
                }
            }, 0, heartbeatInterval, TimeUnit.MILLISECONDS);
            
            logger.info("心跳任务已启动，间隔: {}ms", heartbeatInterval);
        }
    }
    
    /**
     * 停止心跳任务
     */
    private void stopHeartbeatTask() {
        if (heartbeatExecutor != null && !heartbeatExecutor.isShutdown()) {
            heartbeatExecutor.shutdown();
            try {
                if (!heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                heartbeatExecutor.shutdownNow();
            }
            heartbeatExecutor = null;
            logger.info("心跳任务已停止");
        }
    }
    
    /**
     * 发送心跳
     */
    private void sendHeartbeat() {
        if (heartbeatDetector != null) {
            Map<String, Object> heartbeatInfo = new HashMap<>();
            heartbeatInfo.put("nodeId", nodeId);
            heartbeatInfo.put("isMaster", masterElectionService.isMaster());
            heartbeatInfo.put("heartbeatTime", System.currentTimeMillis());
            
            // 注册或更新心跳
            heartbeatDetector.updateHeartbeat(nodeId, heartbeatInfo);
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
        return masterElectionService != null && masterElectionService.isMaster();
    }
    
    /**
     * 获取当前Master节点ID
     * @return Master节点ID
     */
    public String getCurrentMaster() {
        return masterElectionService != null ? masterElectionService.getCurrentMaster() : null;
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
} 