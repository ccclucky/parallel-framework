package pers.cclucky.parallel.core.storage;

import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.api.TaskSlice;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 任务存储接口
 * 定义任务状态和结果的存储操作
 */
public interface TaskStorage {
    
    /**
     * 保存任务状态
     * @param taskId 任务ID
     * @param status 任务状态
     * @param metadata 任务元数据
     * @return 是否成功
     */
    boolean saveTaskStatus(String taskId, String status, Map<String, Object> metadata);
    
    /**
     * 更新任务状态
     * @param taskId 任务ID
     * @param status 任务状态
     * @return 是否成功
     */
    boolean updateTaskStatus(String taskId, String status);
    
    /**
     * 获取任务状态
     * @param taskId 任务ID
     * @return 任务状态
     */
    String getTaskStatus(String taskId);
    
    /**
     * 获取任务元数据
     * @param taskId 任务ID
     * @return 任务元数据
     */
    Map<String, Object> getTaskMetadata(String taskId);
    
    /**
     * 保存任务分片信息
     * @param taskId 任务ID
     * @param slices 任务分片列表
     * @param <T> 分片数据类型
     * @return 是否成功
     */
    <T> boolean saveTaskSlices(String taskId, List<TaskSlice<T>> slices);
    
    /**
     * 获取任务分片信息
     * @param taskId 任务ID
     * @param <T> 分片数据类型
     * @return 任务分片列表
     */
    <T> List<TaskSlice<T>> getTaskSlices(String taskId);
    
    /**
     * 保存分片执行结果
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param result 执行结果
     * @param <R> 结果数据类型
     * @return 是否成功
     */
    <R> boolean saveSliceResult(String taskId, String sliceId, TaskResult<R> result);
    
    /**
     * 获取分片执行结果
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param <R> 结果数据类型
     * @return 执行结果
     */
    <R> TaskResult<R> getSliceResult(String taskId, String sliceId);
    
    /**
     * 获取任务所有分片的执行结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 所有分片的执行结果
     */
    <R> List<TaskResult<R>> getAllSliceResults(String taskId);
    
    /**
     * 保存任务最终结果
     * @param taskId 任务ID
     * @param result 任务结果
     * @param <R> 结果数据类型
     * @return 是否成功
     */
    <R> boolean saveTaskResult(String taskId, TaskResult<R> result);
    
    /**
     * 获取任务最终结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    <R> TaskResult<R> getTaskResult(String taskId);
    
    /**
     * 保存任务进度
     * @param taskId 任务ID
     * @param progress 进度值(0-100)
     * @param message 进度消息
     * @return 是否成功
     */
    boolean saveTaskProgress(String taskId, int progress, String message);
    
    /**
     * 获取任务进度
     * @param taskId 任务ID
     * @return 进度值(0-100)
     */
    int getTaskProgress(String taskId);
    
    /**
     * 获取任务进度消息
     * @param taskId 任务ID
     * @return 进度消息
     */
    String getTaskProgressMessage(String taskId);
    
    /**
     * 分配任务分片到Worker节点
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @param workerId Worker节点ID
     * @return 是否成功
     */
    boolean assignSliceToWorker(String taskId, String sliceId, String workerId);
    
    /**
     * 获取分片分配的Worker节点
     * @param taskId 任务ID
     * @param sliceId 分片ID
     * @return Worker节点ID
     */
    String getSliceWorker(String taskId, String sliceId);
    
    /**
     * 获取Worker节点的所有分片
     * @param workerId Worker节点ID
     * @return 任务ID和分片ID的列表
     */
    List<Map<String, String>> getWorkerSlices(String workerId);
    
    /**
     * 移除任务数据
     * @param taskId 任务ID
     * @return 是否成功
     */
    boolean removeTask(String taskId);
    
    /**
     * 获取运行中的任务列表
     * @return 任务ID列表
     */
    List<String> getRunningTasks();
    
    /**
     * 获取Worker节点的心跳信息
     * @param workerId Worker节点ID
     * @return 心跳信息
     */
    Map<String, Object> getWorkerHeartbeat(String workerId);
    
    /**
     * 更新Worker节点的心跳信息
     * @param workerId Worker节点ID
     * @param heartbeatInfo 心跳信息
     * @return 是否成功
     */
    boolean updateWorkerHeartbeat(String workerId, Map<String, Object> heartbeatInfo);
    
    /**
     * 获取所有已注册的Worker节点ID
     * @return Worker节点ID集合
     */
    Set<String> getAllRegisteredWorkers();
    
    /**
     * 注册Worker节点
     * @param workerId Worker节点ID
     * @param initialInfo 初始心跳信息
     * @return 是否成功
     */
    boolean registerWorker(String workerId, Map<String, Object> initialInfo);
    
    /**
     * 清理任务分片数据
     * 保留最终结果和基本元数据，但删除中间分片数据和结果
     */
    boolean cleanupTaskSlices(String taskId);
    
    /**
     * 清理Worker相关的分片数据
     */
    boolean cleanupWorkerSlices(String workerId, String taskId);
    
    /**
     * 设置数据过期时间
     */
    boolean setTaskDataExpiry(String taskId, long expiryTimeInSeconds);
    
    /**
     * 清理过期的任务数据
     */
    int cleanupExpiredTasks();
} 