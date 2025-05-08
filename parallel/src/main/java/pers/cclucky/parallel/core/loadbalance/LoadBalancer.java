package pers.cclucky.parallel.core.loadbalance;

import java.util.List;

/**
 * 负载均衡器接口
 * 用于在可用节点中选择一个节点执行任务
 */
public interface LoadBalancer {
    
    /**
     * 从可用节点列表中选择一个节点
     * @param availableNodes 可用节点列表
     * @param key 路由键（可以是任务ID、分片ID等）
     * @return 选择的节点ID
     */
    String selectNode(List<String> availableNodes, String key);
    
    /**
     * 从可用节点列表中选择一个节点（带权重）
     * @param availableNodes 可用节点列表
     * @param weights 节点权重映射
     * @param key 路由键（可以是任务ID、分片ID等）
     * @return 选择的节点ID
     */
    String selectNode(List<String> availableNodes, List<Integer> weights, String key);
    
    /**
     * 获取负载均衡器类型名称
     * @return 负载均衡器类型名称
     */
    String getType();
} 