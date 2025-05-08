package pers.cclucky.parallel.core.heartbeat;

import java.util.Map;

/**
 * Worker节点失效监听器接口
 * 用于处理Worker节点失效事件
 */
public interface WorkerFailureListener {
    
    /**
     * 当Worker节点失效时触发
     * @param workerId 失效的Worker节点ID
     * @param lastHeartbeat 最后一次心跳信息
     */
    void onWorkerFailure(String workerId, Map<String, Object> lastHeartbeat);
} 