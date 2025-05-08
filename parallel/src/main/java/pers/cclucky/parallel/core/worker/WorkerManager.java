package pers.cclucky.parallel.core.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.core.storage.TaskStorage;

/**
 * Worker管理器，负责Worker的生命周期管理
 */
public class WorkerManager {
    private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);
    
    private final String nodeId;
    private final TaskStorage taskStorage;
    private Worker worker;
    
    public WorkerManager(String nodeId, TaskStorage taskStorage) {
        this.nodeId = nodeId;
        this.taskStorage = taskStorage;
    }
    
    /**
     * 启动Worker管理器
     */
    public void start() {
        logger.info("启动Worker管理器: {}", nodeId);
        
        // 创建本地Worker
        worker = new LocalWorker(nodeId, taskStorage);
        worker.start();
    }
    
    /**
     * 停止Worker管理器
     */
    public void stop() {
        logger.info("停止Worker管理器: {}", nodeId);
        
        if (worker != null) {
            worker.stop();
            worker = null;
        }
    }
    
    /**
     * 获取Worker
     */
    public Worker getWorker() {
        return worker;
    }
}
