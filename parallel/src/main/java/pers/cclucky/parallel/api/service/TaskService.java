package pers.cclucky.parallel.api.service;

import pers.cclucky.parallel.api.Task;
import pers.cclucky.parallel.api.TaskResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 任务服务接口
 * 提供任务提交和查询的RPC方法
 */
public interface TaskService {
    
    /**
     * 提交任务
     * @param task 任务对象
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 任务ID
     */
    <T, R> String submitTask(Task<T, R> task);
    
    /**
     * 异步提交任务
     * @param task 任务对象
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 包含任务ID的CompletableFuture
     */
    <T, R> CompletableFuture<String> submitTaskAsync(Task<T, R> task);
    
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
     * 获取任务进度
     * @param taskId 任务ID
     * @return 任务进度(0-100)
     */
    int getTaskProgress(String taskId);
    
    /**
     * 批量获取任务状态
     * @param taskIds 任务ID列表
     * @return 任务ID到状态的映射
     */
    List<String> batchGetTaskStatus(List<String> taskIds);
} 