package pers.cclucky.parallel.core.loadbalance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 一致性哈希负载均衡实现
 * @param <T> 节点类型
 */
public class ConsistentHash<T> {
    private static final Logger logger = LoggerFactory.getLogger(ConsistentHash.class);
    
    // 默认虚拟节点数量
    private static final int DEFAULT_VIRTUAL_NODES = 100;
    
    // 哈希环
    private final SortedMap<Integer, T> circle = new TreeMap<>();
    
    // 节点到虚拟节点数量的映射
    private final Map<T, Integer> nodeVirtualCount = new ConcurrentHashMap<>();
    
    // 读写锁，保证线程安全
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    // 哈希函数
    private final HashFunction hashFunction;
    
    /**
     * 默认构造函数，使用MurmurHash算法
     */
    public ConsistentHash() {
        this.hashFunction = new MurmurHash();
    }
    
    /**
     * 使用指定哈希函数构造
     * @param hashFunction 哈希函数
     */
    public ConsistentHash(HashFunction hashFunction) {
        this.hashFunction = hashFunction;
    }
    
    /**
     * 添加节点
     * @param node 节点
     */
    public void addNode(T node) {
        addNode(node, DEFAULT_VIRTUAL_NODES);
    }
    
    /**
     * 添加节点，指定虚拟节点数量
     * @param node 节点
     * @param virtualNodes 虚拟节点数量
     */
    public void addNode(T node, int virtualNodes) {
        if (node == null) {
            return;
        }
        
        try {
            lock.writeLock().lock();
            
            // 记录节点的虚拟节点数量
            nodeVirtualCount.put(node, virtualNodes);
            
            // 为节点创建虚拟节点
            for (int i = 0; i < virtualNodes; i++) {
                String virtualNodeName = node.toString() + "#" + i;
                int hashCode = hashFunction.hash(virtualNodeName);
                circle.put(hashCode, node);
            }
            
            logger.debug("添加节点 {} 及其 {} 个虚拟节点到哈希环", node, virtualNodes);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 移除节点
     * @param node 节点
     */
    public void removeNode(T node) {
        if (node == null) {
            return;
        }
        
        try {
            lock.writeLock().lock();
            
            // 获取节点的虚拟节点数量
            Integer virtualNodes = nodeVirtualCount.remove(node);
            if (virtualNodes == null) {
                return;
            }
            
            // 移除所有虚拟节点
            for (int i = 0; i < virtualNodes; i++) {
                String virtualNodeName = node.toString() + "#" + i;
                int hashCode = hashFunction.hash(virtualNodeName);
                circle.remove(hashCode);
            }
            
            logger.debug("从哈希环移除节点 {} 及其 {} 个虚拟节点", node, virtualNodes);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 更新节点的虚拟节点数量
     * @param node 节点
     * @param newVirtualCount 新的虚拟节点数量
     */
    public void updateNodeWeight(T node, int newVirtualCount) {
        if (node == null || newVirtualCount <= 0) {
            return;
        }
        
        try {
            lock.writeLock().lock();
            
            // 获取旧的虚拟节点数量
            Integer oldVirtualCount = nodeVirtualCount.get(node);
            if (oldVirtualCount == null) {
                // 节点不存在，直接添加
                addNode(node, newVirtualCount);
                return;
            }
            
            if (oldVirtualCount == newVirtualCount) {
                // 虚拟节点数量相同，无需更新
                return;
            }
            
            // 先移除旧的虚拟节点
            for (int i = 0; i < oldVirtualCount; i++) {
                String virtualNodeName = node.toString() + "#" + i;
                int hashCode = hashFunction.hash(virtualNodeName);
                circle.remove(hashCode);
            }
            
            // 再添加新的虚拟节点
            nodeVirtualCount.put(node, newVirtualCount);
            for (int i = 0; i < newVirtualCount; i++) {
                String virtualNodeName = node.toString() + "#" + i;
                int hashCode = hashFunction.hash(virtualNodeName);
                circle.put(hashCode, node);
            }
            
            logger.debug("更新节点 {} 的虚拟节点数量：{} -> {}", node, oldVirtualCount, newVirtualCount);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 获取负责处理指定键的节点
     * @param key 键
     * @return 节点，如果环为空则返回null
     */
    public T getNode(String key) {
        if (key == null) {
            return null;
        }
        
        try {
            lock.readLock().lock();
            
            if (circle.isEmpty()) {
                return null;
            }
            
            int hashCode = hashFunction.hash(key);
            
            // 如果没有大于等于该hashCode的节点，则使用环中第一个节点
            if (!circle.containsKey(hashCode)) {
                SortedMap<Integer, T> tailMap = circle.tailMap(hashCode);
                hashCode = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            }
            
            return circle.get(hashCode);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取所有节点
     * @return 节点集合
     */
    public Collection<T> getAllNodes() {
        try {
            lock.readLock().lock();
            return nodeVirtualCount.keySet();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 获取节点数量
     * @return 节点数量
     */
    public int size() {
        try {
            lock.readLock().lock();
            return nodeVirtualCount.size();
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 是否包含指定节点
     * @param node 节点
     * @return 是否包含
     */
    public boolean containsNode(T node) {
        try {
            lock.readLock().lock();
            return nodeVirtualCount.containsKey(node);
        } finally {
            lock.readLock().unlock();
        }
    }
    
    /**
     * 清空哈希环
     */
    public void clear() {
        try {
            lock.writeLock().lock();
            circle.clear();
            nodeVirtualCount.clear();
            logger.debug("哈希环已清空");
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * 哈希函数接口
     */
    public interface HashFunction {
        /**
         * 计算哈希值
         * @param key 键
         * @return 哈希值
         */
        int hash(String key);
    }
    
    /**
     * MurmurHash算法实现
     */
    public static class MurmurHash implements HashFunction {
        @Override
        public int hash(String key) {
            byte[] bytes = key.getBytes();
            return hash(bytes, bytes.length, 0x1234ABCD);
        }
        
        private int hash(byte[] data, int length, int seed) {
            final int c1 = 0xcc9e2d51;
            final int c2 = 0x1b873593;
            final int r1 = 15;
            final int r2 = 13;
            final int m = 5;
            final int n = 0xe6546b64;
            
            int hash = seed;
            int len = length;
            int roundedEnd = length & 0xfffffffc; // 舍去余数
            
            for (int i = 0; i < roundedEnd; i += 4) {
                int k = (data[i] & 0xff) |
                        ((data[i + 1] & 0xff) << 8) |
                        ((data[i + 2] & 0xff) << 16) |
                        ((data[i + 3] & 0xff) << 24);
                
                k *= c1;
                k = (k << r1) | (k >>> (32 - r1));
                k *= c2;
                
                hash ^= k;
                hash = (hash << r2) | (hash >>> (32 - r2));
                hash = hash * m + n;
            }
            
            int k1 = 0;
            switch (length & 0x03) {
                case 3:
                    k1 = (data[roundedEnd + 2] & 0xff) << 16;
                case 2:
                    k1 |= (data[roundedEnd + 1] & 0xff) << 8;
                case 1:
                    k1 |= data[roundedEnd] & 0xff;
                    k1 *= c1;
                    k1 = (k1 << r1) | (k1 >>> (32 - r1));
                    k1 *= c2;
                    hash ^= k1;
            }
            
            hash ^= length;
            hash ^= (hash >>> 16);
            hash *= 0x85ebca6b;
            hash ^= (hash >>> 13);
            hash *= 0xc2b2ae35;
            hash ^= (hash >>> 16);
            
            return hash;
        }
    }
} 