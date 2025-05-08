package pers.cclucky.parallel.example;

import pers.cclucky.parallel.ParallelFramework;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * 并行计算框架启动器示例
 * 演示如何配置和启动框架
 */
public class ParallelFrameworkLauncher {
    
    /**
     * 主方法
     * @param args 命令行参数
     * @throws Exception 执行异常
     */
    public static void main(String[] args) throws Exception {
        // 解析命令行参数
        String zkAddress = getArgValue(args, "--zk", "127.0.0.1:2181");
        String redisHost = getArgValue(args, "--redis-host", "127.0.0.1");
        String redisPort = getArgValue(args, "--redis-port", "6379");
        String nodeId = getArgValue(args, "--node-id", "");
        
        // 打印配置信息
        System.out.println("启动并行计算框架...");
        System.out.println("ZooKeeper地址: " + zkAddress);
        System.out.println("Redis地址: " + redisHost + ":" + redisPort);
        
        // 创建配置文件
        File configFile = createConfigFile(zkAddress, redisHost, redisPort, nodeId);
        
        // 创建并初始化框架
        try {
            ParallelFramework framework = new ParallelFramework(configFile.getAbsolutePath());
            framework.init();
            framework.start();
            
            System.out.println("框架已启动，节点ID: " + framework.getNodeId());
            System.out.println("当前Master节点: " + framework.getCurrentMaster());
            System.out.println("本节点是否为Master: " + framework.isMaster());
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("正在关闭框架...");
                framework.stop();
                System.out.println("框架已关闭");
            }));
            
            // 保持程序运行
            System.out.println("按Ctrl+C停止程序");
            Thread.currentThread().join();
        } catch (Exception e) {
            System.err.println("启动框架失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 获取命令行参数值
     * @param args 命令行参数数组
     * @param name 参数名
     * @param defaultValue 默认值
     * @return 参数值
     */
    private static String getArgValue(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
    
    /**
     * 创建配置文件
     * @param zkAddress ZooKeeper地址
     * @param redisHost Redis主机
     * @param redisPort Redis端口
     * @param nodeId 节点ID
     * @return 配置文件
     * @throws IOException 文件操作异常
     */
    private static File createConfigFile(String zkAddress, String redisHost, String redisPort, String nodeId) throws IOException {
        Properties props = new Properties();
        
        // 应用配置
        props.setProperty("app.name", "parallel-framework");
        props.setProperty("app.version", "1.0.0");
        
        // ZooKeeper配置
        props.setProperty("zookeeper.address", zkAddress);
        props.setProperty("zookeeper.session.timeout", "30000");
        props.setProperty("zookeeper.connection.timeout", "10000");
        
        // Redis配置
        props.setProperty("spring.redis.host", redisHost);
        props.setProperty("spring.redis.port", redisPort);
        props.setProperty("spring.redis.database", "0");
        
        // Dubbo配置
        props.setProperty("dubbo.registry.address", "zookeeper://" + zkAddress);
        
        // 节点配置
        if (nodeId != null && !nodeId.isEmpty()) {
            props.setProperty("node.id", nodeId);
        }
        
        // 创建临时配置文件
        File configFile = File.createTempFile("parallel-", ".properties");
        try (FileOutputStream fos = new FileOutputStream(configFile)) {
            props.store(fos, "Parallel Framework Configuration");
        }
        
        // 确保程序退出时删除配置文件
        configFile.deleteOnExit();
        
        return configFile;
    }
    
    /**
     * 打印使用帮助
     */
    private static void printUsage() {
        System.out.println("并行计算框架启动器");
        System.out.println("用法: java ParallelFrameworkLauncher [选项]");
        System.out.println("选项:");
        System.out.println("  --zk <地址>            ZooKeeper地址，默认127.0.0.1:2181");
        System.out.println("  --redis-host <主机>    Redis主机地址，默认127.0.0.1");
        System.out.println("  --redis-port <端口>    Redis端口，默认6379");
        System.out.println("  --node-id <ID>         节点ID，默认自动生成");
        System.out.println("示例:");
        System.out.println("  java ParallelFrameworkLauncher --zk 192.168.1.100:2181 --redis-host 192.168.1.100");
    }
} 