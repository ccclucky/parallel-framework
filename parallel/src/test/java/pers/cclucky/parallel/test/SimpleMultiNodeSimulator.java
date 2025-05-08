package pers.cclucky.parallel.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.example.SimpleMasterNode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简化版多节点模拟器
 * 在单机上模拟多个节点运行，实现Master选举测试
 */
public class SimpleMultiNodeSimulator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMultiNodeSimulator.class);
    
    private final int nodeCount;
    private final String baseDir;
    private final List<SimpleMasterNode> nodes = new ArrayList<>();
    private final ExecutorService executorService;
    private final AtomicInteger activeNodeCount = new AtomicInteger(0);
    private boolean running = false;
    
    /**
     * 构造多节点模拟器
     * 
     * @param nodeCount 节点数量
     * @param baseDir 基础目录，用于存储每个节点的配置和数据
     */
    public SimpleMultiNodeSimulator(int nodeCount, String baseDir) {
        this.nodeCount = nodeCount;
        this.baseDir = baseDir;
        this.executorService = Executors.newFixedThreadPool(nodeCount);
        
        // 创建基础目录
        File baseDirFile = new File(baseDir);
        if (!baseDirFile.exists()) {
            baseDirFile.mkdirs();
        }
    }
    
    /**
     * 初始化模拟环境
     * 
     * @throws IOException 如果配置文件操作失败
     */
    public void initialize() throws IOException {
        LOGGER.info("初始化{}个节点的模拟环境", nodeCount);
        
        // 读取基础配置文件
        Properties baseProperties = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                baseProperties.load(is);
            } else {
                // 如果没有找到基础配置，创建默认配置
                baseProperties.setProperty("zookeeper.address", "127.0.0.1:2181");
                baseProperties.setProperty("task.processor.count", "4");
            }
        }
        
        // 为每个节点创建目录和配置
        for (int i = 0; i < nodeCount; i++) {
            String nodeId = "node-" + i;
            String nodeDirPath = baseDir + File.separator + nodeId;
            File nodeDir = new File(nodeDirPath);
            if (!nodeDir.exists()) {
                nodeDir.mkdirs();
            }
            
            // 创建节点特定配置
            Properties nodeProperties = new Properties();
            nodeProperties.putAll(baseProperties);
            
            // 设置节点特定属性
            nodeProperties.setProperty("node.id", nodeId);
            nodeProperties.setProperty("node.port", String.valueOf(8080 + i));
            nodeProperties.setProperty("node.weight", String.valueOf(10)); // 默认权重
            
            // 保存配置文件
            File configFile = new File(nodeDirPath, "application.properties");
            try (FileOutputStream fos = new FileOutputStream(configFile)) {
                nodeProperties.store(fos, "Node " + nodeId + " Configuration");
            }
            
            LOGGER.info("节点{}配置已创建: {}", nodeId, nodeDirPath);
        }
    }
    
    /**
     * 启动所有模拟节点
     */
    public void startAllNodes() {
        if (running) {
            LOGGER.warn("节点已经在运行中，请先停止");
            return;
        }
        
        LOGGER.info("启动{}个模拟节点", nodeCount);
        
        CountDownLatch startLatch = new CountDownLatch(nodeCount);
        
        for (int i = 0; i < nodeCount; i++) {
            final int nodeIndex = i;
            executorService.submit(() -> {
                try {
                    String nodeId = "node-" + nodeIndex;
                    String nodeDirPath = baseDir + File.separator + nodeId;
                    
                    // 设置节点特定的系统属性
                    System.setProperty("config.dir", nodeDirPath);
                    
                    // 获取ZooKeeper地址
                    String zkAddress = "127.0.0.1:2181"; // 默认地址
                    File configFile = new File(nodeDirPath, "application.properties");
                    if (configFile.exists()) {
                        Properties props = new Properties();
                        try (InputStream is = Files.newInputStream(configFile.toPath())) {
                            props.load(is);
                            zkAddress = props.getProperty("zookeeper.address", zkAddress);
                        }
                    }
                    
                    // 创建并初始化简化版主节点
                    SimpleMasterNode node = new SimpleMasterNode(nodeId, zkAddress);
                    node.init();
                    node.start();
                    
                    nodes.add(node);
                    activeNodeCount.incrementAndGet();
                    
                    LOGGER.info("节点{}已启动", nodeId);
                } catch (Exception e) {
                    LOGGER.error("启动节点{}失败", "node-" + nodeIndex, e);
                } finally {
                    startLatch.countDown();
                }
            });
        }
        
        try {
            // 等待所有节点启动或超时
            boolean allStarted = startLatch.await(2, TimeUnit.MINUTES);
            if (!allStarted) {
                LOGGER.warn("部分节点启动超时");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("等待节点启动被中断", e);
        }
        
        running = true;
        LOGGER.info("已成功启动{}个节点 (共{}个)", activeNodeCount.get(), nodeCount);
    }
    
    /**
     * 停止所有模拟节点
     */
    public void stopAllNodes() {
        if (!running) {
            LOGGER.warn("节点尚未运行");
            return;
        }
        
        LOGGER.info("停止所有模拟节点");
        
        for (SimpleMasterNode node : nodes) {
            try {
                node.stop();
            } catch (Exception e) {
                LOGGER.error("停止节点{}失败", node.getNodeId(), e);
            }
        }
        
        nodes.clear();
        activeNodeCount.set(0);
        
        // 关闭执行器服务
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        running = false;
        LOGGER.info("所有模拟节点已停止");
    }
    
    /**
     * 获取活跃节点数量
     * 
     * @return 当前活跃的节点数量
     */
    public int getActiveNodeCount() {
        return activeNodeCount.get();
    }
    
    /**
     * 获取指定索引的节点实例
     * 
     * @param index 节点索引
     * @return 对应的节点实例，如果索引无效返回null
     */
    public SimpleMasterNode getNode(int index) {
        if (index >= 0 && index < nodes.size()) {
            return nodes.get(index);
        }
        return null;
    }
    
    /**
     * 获取所有节点实例
     * 
     * @return 所有节点实例的列表
     */
    public List<SimpleMasterNode> getAllNodes() {
        return new ArrayList<>(nodes);
    }
    
    /**
     * 检查是否正在运行
     * 
     * @return 是否正在运行
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * 清理模拟环境
     */
    public void cleanup() {
        if (running) {
            stopAllNodes();
        }
        
        // 清理测试目录
        try {
            // 递归删除目录内容
            Path directory = Paths.get(baseDir);
            if (Files.exists(directory)) {
                Files.walk(directory)
                     .sorted((p1, p2) -> -p1.compareTo(p2)) // 反向排序，先删除文件后删除目录
                     .forEach(path -> {
                         try {
                             Files.delete(path);
                         } catch (IOException e) {
                             LOGGER.warn("删除文件失败: {}", path, e);
                         }
                     });
            }
            LOGGER.info("测试环境已清理");
        } catch (IOException e) {
            LOGGER.error("清理测试环境失败", e);
        }
    }
    
    /**
     * 获取Master节点实例
     * 
     * @return Master节点实例，如果未找到返回null
     */
    public SimpleMasterNode getMasterNode() {
        for (SimpleMasterNode node : nodes) {
            if (node.isMaster()) {
                return node;
            }
        }
        return null;
    }
    
    /**
     * 获取当前主节点ID
     * 
     * @return 主节点ID，如果未找到返回null
     */
    public String getCurrentMasterId() {
        if (nodes.isEmpty()) {
            return null;
        }
        return nodes.get(0).getCurrentMaster();
    }
} 