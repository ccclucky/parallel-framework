package pers.cclucky.parallel.core.loadbalance;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 一致性哈希负载均衡器实现
 * 使用一致性哈希算法选择节点，确保相同的key尽可能路由到相同的节点
 */
public class ConsistentHashLoadBalancer implements LoadBalancer {
    
    // 每个节点的虚拟节点数，越大分布越均匀，但内存占用越高
    private static final int DEFAULT_VIRTUAL_NODE_COUNT = 160;
    
    // 一致性哈希环，使用TreeMap保证有序，key是hash值，value是实际节点ID
    private final ConcurrentSkipListMap<Long, String> hashRing = new ConcurrentSkipListMap<>();
    
    // 记录每个实际节点对应的所有虚拟节点hash值
    private final Map<String, Set<Long>> nodeToHashes = new ConcurrentHashMap<>();
    
    // 虚拟节点数
    private final int virtualNodeCount;
    
    // 节点权重缓存
    private final Map<String, Integer> weightCache = new ConcurrentHashMap<>();
    
    /**
     * 创建一致性哈希负载均衡器（使用默认虚拟节点数）
     */
    public ConsistentHashLoadBalancer() {
        this(DEFAULT_VIRTUAL_NODE_COUNT);
    }
    
    /**
     * 创建一致性哈希负载均衡器
     * @param virtualNodeCount 虚拟节点数
     */
    public ConsistentHashLoadBalancer(int virtualNodeCount) {
        this.virtualNodeCount = virtualNodeCount;
    }
    
    @Override
    public String selectNode(List<String> availableNodes, String key) {
        if (availableNodes == null || availableNodes.isEmpty()) {
            return null;
        }
        
        if (availableNodes.size() == 1) {
            return availableNodes.get(0);
        }
        
        // 重建哈希环（如果节点列表变化）
        rebuildHashRingIfNeeded(availableNodes);
        
        // 计算key的hash值
        long hash = hash(key);
        
        // 在哈希环上查找第一个大于等于hash值的节点
        Map.Entry<Long, String> entry = hashRing.ceilingEntry(hash);
        
        // 如果没有找到，则返回哈希环上的第一个节点
        if (entry == null) {
            entry = hashRing.firstEntry();
        }
        
        return entry != null ? entry.getValue() : availableNodes.get(0);
    }
    
    @Override
    public String selectNode(List<String> availableNodes, List<Integer> weights, String key) {
        if (availableNodes == null || availableNodes.isEmpty()) {
            return null;
        }
        
        if (availableNodes.size() == 1) {
            return availableNodes.get(0);
        }
        
        // 更新权重缓存
        updateWeightCache(availableNodes, weights);
        
        // 重建哈希环（考虑权重）
        rebuildHashRingWithWeights(availableNodes);
        
        // 计算key的hash值
        long hash = hash(key);
        
        // 在哈希环上查找第一个大于等于hash值的节点
        Map.Entry<Long, String> entry = hashRing.ceilingEntry(hash);
        
        // 如果没有找到，则返回哈希环上的第一个节点
        if (entry == null) {
            entry = hashRing.firstEntry();
        }
        
        return entry != null ? entry.getValue() : availableNodes.get(0);
    }
    
    @Override
    public String getType() {
        return "ConsistentHash";
    }
    
    /**
     * 如果节点列表变化，则重建哈希环
     * @param availableNodes 可用节点列表
     */
    private void rebuildHashRingIfNeeded(List<String> availableNodes) {
        // 检查是否需要重建哈希环
        boolean needRebuild = false;
        
        // 检查是否有新增节点
        for (String node : availableNodes) {
            if (!nodeToHashes.containsKey(node)) {
                needRebuild = true;
                break;
            }
        }
        
        // 检查是否有移除节点
        if (!needRebuild) {
            for (String node : nodeToHashes.keySet()) {
                if (!availableNodes.contains(node)) {
                    needRebuild = true;
                    break;
                }
            }
        }
        
        // 如果需要重建哈希环
        if (needRebuild) {
            rebuildHashRing(availableNodes);
        }
    }
    
    /**
     * 重建哈希环
     * @param availableNodes 可用节点列表
     */
    private void rebuildHashRing(List<String> availableNodes) {
        // 清空哈希环
        hashRing.clear();
        
        // 清空节点到哈希值的映射
        nodeToHashes.clear();
        
        // 为每个节点添加虚拟节点
        for (String node : availableNodes) {
            addNode(node);
        }
    }
    
    /**
     * 使用权重重建哈希环
     * @param availableNodes 可用节点列表
     */
    private void rebuildHashRingWithWeights(List<String> availableNodes) {
        // 清空哈希环
        hashRing.clear();
        
        // 清空节点到哈希值的映射
        nodeToHashes.clear();
        
        // 为每个节点添加虚拟节点（考虑权重）
        for (String node : availableNodes) {
            int weight = weightCache.getOrDefault(node, 1);
            addNode(node, weight);
        }
    }
    
    /**
     * 更新权重缓存
     * @param availableNodes 可用节点列表
     * @param weights 节点权重列表
     */
    private void updateWeightCache(List<String> availableNodes, List<Integer> weights) {
        if (weights != null && weights.size() == availableNodes.size()) {
            for (int i = 0; i < availableNodes.size(); i++) {
                String node = availableNodes.get(i);
                int weight = Math.max(1, weights.get(i)); // 确保权重至少为1
                weightCache.put(node, weight);
            }
        } else {
            // 如果权重列表无效，使用默认权重1
            for (String node : availableNodes) {
                weightCache.putIfAbsent(node, 1);
            }
        }
    }
    
    /**
     * 添加节点到哈希环
     * @param node 节点ID
     */
    private void addNode(String node) {
        addNode(node, 1);
    }
    
    /**
     * 添加节点到哈希环（带权重）
     * @param node 节点ID
     * @param weight 节点权重
     */
    private void addNode(String node, int weight) {
        int actualVirtualNodes = virtualNodeCount * weight;
        Set<Long> hashes = new HashSet<>();
        
        for (int i = 0; i < actualVirtualNodes; i++) {
            String virtualNodeName = node + "#" + i;
            long hash = hash(virtualNodeName);
            hashRing.put(hash, node);
            hashes.add(hash);
        }
        
        nodeToHashes.put(node, hashes);
    }
    
    /**
     * 计算哈希值（使用FNV1_32_HASH算法）
     * @param key 键
     * @return 哈希值
     */
    private long hash(String key) {
        final int p = 16777619;
        long hash = 2166136261L;
        for (int i = 0; i < key.length(); i++) {
            hash = (hash ^ key.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        
        // 确保hash值为正数
        return hash & 0x7FFFFFFFFFFFFFFFL;
    }
} 