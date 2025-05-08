package pers.cclucky.parallel.api.service;

import pers.cclucky.parallel.api.TaskSlice;
import pers.cclucky.parallel.api.TaskResult;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Worker节点服务接口
 * 定义Worker节点的RPC方法
 */
public interface WorkerService {
    
    /**
     * 处理任务分片
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param taskClassName 任务类名
     * @param slice 任务分片对象
     * @param params 任务参数
     * @param <T> 分片数据类型
     * @param <R> 处理结果类型
     * @return 处理结果
     */
    <T, R> TaskResult<R> processSlice(String taskId, String sliceId, String taskClassName, TaskSlice<T> slice, Map<String, Object> params);
    
    /**
     * 异步处理任务分片
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param taskClassName 任务类名
     * @param slice 任务分片对象
     * @param params 任务参数
     * @param <T> 分片数据类型
     * @param <R> 处理结果类型
     * @return 包含处理结果的CompletableFuture
     */
    <T, R> CompletableFuture<TaskResult<R>> processSliceAsync(String taskId, String sliceId, String taskClassName, TaskSlice<T> slice, Map<String, Object> params);
    
    /**
     * 注册Worker节点
     * @param workerId 节点ID
     * @param capacityInfo 节点容量信息
     * @return 是否注册成功
     */
    boolean register(String workerId, Map<String, Object> capacityInfo);
    
    /**
     * 发送心跳
     * @param workerId 节点ID
     * @param loadInfo 节点负载信息
     * @return 心跳响应
     */
    Map<String, Object> heartbeat(String workerId, Map<String, Object> loadInfo);
    
    /**
     * 获取Worker节点状态
     * @return 节点状态信息
     */
    Map<String, Object> getStatus();
    
    /**
     * 取消正在执行的任务分片
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @return 是否成功取消
     */
    boolean cancelSlice(String taskId, String sliceId);
} 