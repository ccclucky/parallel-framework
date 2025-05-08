package pers.cclucky.parallel.core.heartbeat;

import java.util.Map;
import java.util.Set;

/**
 * 心跳检测器接口
 * 用于检测Worker节点的健康状态
 */
public interface HeartbeatDetector {
    
    /**
     * 启动心跳检测服务
     */
    void start();
    
    /**
     * 停止心跳检测服务
     */
    void stop();
    
    /**
     * 注册Worker节点
     * @param workerId Worker节点ID
     * @param initialInfo 初始节点信息
     * @return 是否注册成功
     */
    boolean registerWorker(String workerId, Map<String, Object> initialInfo);
    
    /**
     * 注销Worker节点
     * @param workerId Worker节点ID
     * @return 是否注销成功
     */
    boolean unregisterWorker(String workerId);
    
    /**
     * 更新Worker节点心跳信息
     * @param workerId Worker节点ID
     * @param heartbeatInfo 心跳信息
     * @return 是否更新成功
     */
    boolean updateHeartbeat(String workerId, Map<String, Object> heartbeatInfo);
    
    /**
     * 检查Worker节点是否存活
     * @param workerId Worker节点ID
     * @return 是否存活
     */
    boolean isWorkerAlive(String workerId);
    
    /**
     * 获取所有存活的Worker节点
     * @return 存活的Worker节点集合
     */
    Set<String> getAliveWorkers();
    
    /**
     * 获取所有失效的Worker节点
     * @return 失效的Worker节点集合
     */
    Set<String> getDeadWorkers();
    
    /**
     * 获取Worker节点的心跳信息
     * @param workerId Worker节点ID
     * @return 心跳信息
     */
    Map<String, Object> getWorkerHeartbeat(String workerId);
    
    /**
     * 添加Worker节点失效监听器
     * @param listener 失效监听器
     */
    void addWorkerFailureListener(WorkerFailureListener listener);
    
    /**
     * 移除Worker节点失效监听器
     * @param listener 失效监听器
     */
    void removeWorkerFailureListener(WorkerFailureListener listener);
} 