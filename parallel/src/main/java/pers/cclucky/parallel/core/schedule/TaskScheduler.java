package pers.cclucky.parallel.core.schedule;

import pers.cclucky.parallel.api.Task;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.api.TaskSlice;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * 任务调度器接口
 * 负责任务的分片、分发和结果收集
 */
public interface TaskScheduler {
    
    /**
     * 提交任务
     * @param task 任务对象
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 任务ID
     */
    <T, R> String submitTask(Task<T, R> task);
    
    /**
     * 提交任务（带参数）
     * @param task 任务对象
     * @param params 任务参数
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 任务ID
     */
    <T, R> String submitTask(Task<T, R> task, Map<String, Object> params);
    
    /**
     * 异步提交任务
     * @param task 任务对象
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 包含任务ID的CompletableFuture
     */
    <T, R> CompletableFuture<String> submitTaskAsync(Task<T, R> task);
    
    /**
     * 异步提交任务（带参数）
     * @param task 任务对象
     * @param params 任务参数
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 包含任务ID的CompletableFuture
     */
    <T, R> CompletableFuture<String> submitTaskAsync(Task<T, R> task, Map<String, Object> params);
    
    /**
     * 获取任务状态
     * @param taskId 任务ID
     * @return 任务状态
     */
    String getTaskStatus(String taskId);
    
    /**
     * 获取任务结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    <R> TaskResult<R> getTaskResult(String taskId);
    
    /**
     * 异步获取任务结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 包含任务结果的CompletableFuture
     */
    <R> CompletableFuture<TaskResult<R>> getTaskResultAsync(String taskId);
    
    /**
     * 取消任务
     * @param taskId 任务ID
     * @return 是否成功取消
     */
    boolean cancelTask(String taskId);
    
    /**
     * 重新调度任务
     * @param taskId 任务ID
     * @return 是否成功重新调度
     */
    boolean rescheduleTask(String taskId);
    
    /**
     * 处理Worker节点失效
     * @param workerId 节点ID
     * @return 重新分配的任务数量
     */
    int handleWorkerFailure(String workerId);
    
    /**
     * 获取任务进度
     * @param taskId 任务ID
     * @return 任务进度(0-100)
     */
    int getTaskProgress(String taskId);
    
    /**
     * 获取分片执行结果
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param <R> 结果数据类型
     * @return 分片执行结果
     */
    <R> TaskResult<R> getSliceResult(String taskId, String sliceId);
    
    /**
     * 获取未完成的分片
     * @param taskId 任务ID
     * @param <T> 任务数据类型
     * @return 未完成的分片列表
     */
    <T> List<TaskSlice<T>> getPendingSlices(String taskId);
    
    /**
     * 任务调度器启动
     */
    void start();
    
    /**
     * 任务调度器停止
     */
    void stop();
} 