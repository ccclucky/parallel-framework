package pers.cclucky.parallel.core.election;

/**
 * Master选举服务接口
 * 定义Master选举相关的操作
 */
public interface MasterElectionService {
    
    /**
     * 启动选举服务
     * 连接ZooKeeper集群并参与Master选举
     */
    void start();
    
    /**
     * 停止选举服务
     * 断开ZooKeeper连接并退出选举
     */
    void stop();
    
    /**
     * 判断当前节点是否为Master
     * @return 是否为Master
     */
    boolean isMaster();
    
    /**
     * 获取当前Master节点ID
     * @return Master节点ID，如果没有Master则返回null
     */
    String getCurrentMaster();
    
    /**
     * 注册Master变更监听器
     * @param listener Master变更监听器
     */
    void registerMasterChangeListener(MasterChangeListener listener);
    
    /**
     * 移除Master变更监听器
     * @param listener Master变更监听器
     */
    void removeMasterChangeListener(MasterChangeListener listener);
    
    /**
     * 释放Master角色
     * 主动放弃Master角色，触发重新选举
     * @return 释放是否成功
     */
    boolean releaseMaster();
} 