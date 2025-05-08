package pers.cclucky.parallel.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.core.election.MasterChangeListener;
import pers.cclucky.parallel.core.election.MasterElectionService;
import pers.cclucky.parallel.core.election.ZookeeperMasterElection;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * 简化版启动器示例
 * 仅使用ZooKeeper进行Master选举，不依赖Redis和Spring
 */
public class SimpleLauncher {
    private static final Logger logger = LoggerFactory.getLogger(SimpleLauncher.class);
    
    public static void main(String[] args) {
        // 解析命令行参数
        String zkAddress = getArgValue(args, "--zk", "127.0.0.1:2181");
        String nodeId = getArgValue(args, "--node-id", "node-" + UUID.randomUUID().toString().substring(0, 8));
        
        System.out.println("启动简化版Master选举...");
        System.out.println("ZooKeeper地址: " + zkAddress);
        System.out.println("节点ID: " + nodeId);
        
        try {
            // 创建Master选举服务
            MasterElectionService electionService = new ZookeeperMasterElection(zkAddress, nodeId);
            
            // 创建锁用于阻塞主线程
            CountDownLatch latch = new CountDownLatch(1);
            
            // 添加Master变更监听器
            electionService.registerMasterChangeListener(new MasterChangeListener() {
                @Override
                public void onBecomeMaster(String nodeId) {
                    System.out.println("【事件通知】节点 " + nodeId + " 成为Master!");
                }
                
                @Override
                public void onLoseMaster(String nodeId) {
                    System.out.println("【事件通知】节点 " + nodeId + " 失去Master角色!");
                }
                
                @Override
                public void onMasterChange(String oldMasterId, String newMasterId) {
                    System.out.println("【事件通知】Master变更: " + oldMasterId + " -> " + newMasterId);
                }
            });
            
            // 启动选举服务
            electionService.start();
            
            System.out.println("Master选举服务已启动");
            System.out.println("本节点是否为Master: " + electionService.isMaster());
            System.out.println("当前Master节点: " + electionService.getCurrentMaster());
            
            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("正在关闭Master选举服务...");
                electionService.stop();
                System.out.println("Master选举服务已关闭");
                latch.countDown();
            }));
            
            // 保持程序运行
            System.out.println("按Ctrl+C停止程序");
            latch.await();
            
        } catch (Exception e) {
            System.err.println("启动Master选举服务失败: " + e.getMessage());
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
} 