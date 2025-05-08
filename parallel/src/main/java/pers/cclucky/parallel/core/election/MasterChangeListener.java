package pers.cclucky.parallel.core.election;

/**
 * Master变更监听器接口
 * 用于监听Master节点变更事件
 */
public interface MasterChangeListener {
    
    /**
     * 当前节点成为Master时触发
     * @param nodeId 当前节点ID
     */
    void onBecomeMaster(String nodeId);
    
    /**
     * 当前节点失去Master角色时触发
     * @param nodeId 当前节点ID
     */
    void onLoseMaster(String nodeId);
    
    /**
     * Master节点发生变更时触发
     * @param oldMasterId 旧Master节点ID
     * @param newMasterId 新Master节点ID
     */
    void onMasterChange(String oldMasterId, String newMasterId);
} 