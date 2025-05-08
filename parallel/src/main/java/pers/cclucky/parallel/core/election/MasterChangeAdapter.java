package pers.cclucky.parallel.core.election;

/**
 * Master变更监听器适配器
 * 提供默认的空实现，方便用户只重写需要的方法
 */
public class MasterChangeAdapter implements MasterChangeListener {
    
    @Override
    public void onBecomeMaster(String nodeId) {
        // 默认空实现
    }
    
    @Override
    public void onLoseMaster(String nodeId) {
        // 默认空实现
    }
    
    @Override
    public void onMasterChange(String oldMasterId, String newMasterId) {
        // 默认空实现
    }
} 