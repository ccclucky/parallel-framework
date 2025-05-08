package pers.cclucky.parallel.core.worker;

import java.util.concurrent.CompletableFuture;

/**
 * Worker接口，负责实际执行任务分片
 */
public interface Worker {
    /**
     * 启动Worker
     */
    void start();
    
    /**
     * 停止Worker
     */
    void stop();
    
    /**
     * 执行任务分片
     * 
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @return 异步执行结果
     */
    CompletableFuture<Boolean> executeSlice(String taskId, String sliceId);
}
