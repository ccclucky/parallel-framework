package pers.cclucky.parallel.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.api.Task;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.api.service.TaskService;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并行计算框架客户端API
 * 提供简单易用的接口调用分布式计算资源
 */
public class ParallelClient {
    private static final Logger logger = LoggerFactory.getLogger(ParallelClient.class);
    
    // 任务服务
    private final TaskService taskService;
    
    // 默认任务超时时间（秒）
    private static final long DEFAULT_TASK_TIMEOUT = 3600;
    
    // 缓存中保存的最大任务Future数量
    private static final int MAX_CACHED_FUTURES = 1000;
    
    // 清理任务间隔（秒）
    private static final int CLEANUP_INTERVAL = 600;
    
    // 完成任务的缓存
    private final Map<String, CompletedTaskInfo<?>> completedTasks;
    
    // 当前执行中的任务Future缓存
    private final Map<String, CompletableFuture<? extends TaskResult<?>>> taskFutures;
    
    // 任务计数器，用于监控缓存大小
    private final AtomicInteger taskCounter;
    
    // 清理线程
    private final ScheduledExecutorService cleanupExecutor;
    
    /**
     * 创建并行计算客户端
     * @param taskService 任务服务实例
     */
    public ParallelClient(TaskService taskService) {
        this.taskService = taskService;
        this.completedTasks = new ConcurrentHashMap<>();
        this.taskFutures = new ConcurrentHashMap<>();
        this.taskCounter = new AtomicInteger(0);
        
        // 创建清理线程
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "parallel-client-cleanup");
            t.setDaemon(true);
            return t;
        });
        
        // 启动定期清理任务
        this.cleanupExecutor.scheduleAtFixedRate(this::cleanupCompletedTasks, 
                CLEANUP_INTERVAL, CLEANUP_INTERVAL, TimeUnit.SECONDS);
    }
    
    /**
     * 客户端关闭，释放资源
     */
    public void close() {
        // 关闭清理线程
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cleanupExecutor.shutdownNow();
        }
        
        // 清理缓存
        completedTasks.clear();
        taskFutures.clear();
    }
    
    /**
     * 提交任务（同步执行）
     * @param task 任务对象
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 任务结果
     * @throws InterruptedException 如果线程被中断
     * @throws ExecutionException 如果任务执行出错
     * @throws TimeoutException 如果任务执行超时
     */
    public <T, R> TaskResult<R> submitAndWait(Task<T, R> task) 
            throws InterruptedException, ExecutionException, TimeoutException {
        return submitAndWait(task, new HashMap<>(), DEFAULT_TASK_TIMEOUT);
    }
    
    /**
     * 提交任务（同步执行）
     * @param task 任务对象
     * @param params 任务参数
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 任务结果
     * @throws InterruptedException 如果线程被中断
     * @throws ExecutionException 如果任务执行出错
     * @throws TimeoutException 如果任务执行超时
     */
    public <T, R> TaskResult<R> submitAndWait(Task<T, R> task, Map<String, Object> params) 
            throws InterruptedException, ExecutionException, TimeoutException {
        return submitAndWait(task, params, DEFAULT_TASK_TIMEOUT);
    }
    
    /**
     * 提交任务（同步执行）
     * @param task 任务对象
     * @param params 任务参数
     * @param timeoutSeconds 超时时间（秒）
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 任务结果
     * @throws InterruptedException 如果线程被中断
     * @throws ExecutionException 如果任务执行出错
     * @throws TimeoutException 如果任务执行超时
     */
    public <T, R> TaskResult<R> submitAndWait(Task<T, R> task, Map<String, Object> params, long timeoutSeconds) 
            throws InterruptedException, ExecutionException, TimeoutException {
        
        // 设置任务参数
        String taskId = task.getTaskId();
        if (taskId == null || taskId.isEmpty()) {
            // 如果任务ID为空，生成一个
            taskId = generateTaskId();
            params.put("_taskId", taskId);
        }
        
        // 检查缓存中是否有已完成的结果
        CompletedTaskInfo<R> cachedResult = (CompletedTaskInfo<R>) completedTasks.get(taskId);
        if (cachedResult != null) {
            logger.debug("从缓存中获取任务结果: taskId={}", taskId);
            return cachedResult.getResult();
        }
        
        // 提交任务
        String submittedTaskId = taskService.submitTask(task);
        
        // 递增任务计数器
        if (taskCounter.incrementAndGet() > MAX_CACHED_FUTURES) {
            // 如果超过阈值，触发清理
            cleanupCompletedTasks();
        }
        
        try {
            // 等待任务完成
            TaskResult<R> result = waitForResult(submittedTaskId, timeoutSeconds);
            
            // 如果任务完成，保存结果到缓存
            if (result != null && result.isCompleted()) {
                completedTasks.put(submittedTaskId, new CompletedTaskInfo<>(result));
            }
            
            return result;
        } finally {
            // 移除正在执行的任务Future
            taskFutures.remove(submittedTaskId);
        }
    }
    
    /**
     * 异步提交任务
     * @param task 任务对象
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 包含任务ID的CompletableFuture
     */
    public <T, R> CompletableFuture<String> submitAsync(Task<T, R> task) {
        return submitAsync(task, new HashMap<>());
    }
    
    /**
     * 异步提交任务
     * @param task 任务对象
     * @param params 任务参数
     * @param <T> 任务数据类型
     * @param <R> 结果数据类型
     * @return 包含任务ID的CompletableFuture
     */
    public <T, R> CompletableFuture<String> submitAsync(Task<T, R> task, Map<String, Object> params) {
        // 设置任务参数
        String taskId = task.getTaskId();
        if (taskId == null || taskId.isEmpty()) {
            // 如果任务ID为空，生成一个
            taskId = generateTaskId();
            params.put("_taskId", taskId);
        }
        
        // 递增任务计数器
        if (taskCounter.incrementAndGet() > MAX_CACHED_FUTURES) {
            // 如果超过阈值，触发清理
            cleanupCompletedTasks();
        }
        
        // 异步提交任务
        return taskService.submitTaskAsync(task);
    }
    
    /**
     * 获取任务状态
     * @param taskId 任务ID
     * @return 任务状态
     */
    public String getTaskStatus(String taskId) {
        return taskService.getTaskStatus(taskId);
    }
    
    /**
     * 获取任务进度
     * @param taskId 任务ID
     * @return 任务进度(0-100)
     */
    public int getTaskProgress(String taskId) {
        return taskService.getTaskProgress(taskId);
    }
    
    /**
     * 获取任务结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 任务结果
     */
    public <R> TaskResult<R> getTaskResult(String taskId) {
        // 检查缓存中是否有结果
        CompletedTaskInfo<R> cachedResult = (CompletedTaskInfo<R>) completedTasks.get(taskId);
        if (cachedResult != null) {
            return cachedResult.getResult();
        }
        
        // 从服务获取结果
        TaskResult<R> result = taskService.getTaskResult(taskId);
        
        // 如果结果已完成，保存到缓存
        if (result != null && result.isCompleted()) {
            completedTasks.put(taskId, new CompletedTaskInfo<>(result));
        }
        
        return result;
    }
    
    /**
     * 异步获取任务结果
     * @param taskId 任务ID
     * @param <R> 结果数据类型
     * @return 包含任务结果的CompletableFuture
     */
    public <R> CompletableFuture<TaskResult<R>> getTaskResultAsync(String taskId) {
        // 检查是否已有Future
        CompletableFuture<TaskResult<R>> future = (CompletableFuture<TaskResult<R>>) taskFutures.get(taskId);
        if (future != null) {
            return future;
        }
        
        // 检查缓存中是否有结果
        CompletedTaskInfo<R> cachedResult = (CompletedTaskInfo<R>) completedTasks.get(taskId);
        if (cachedResult != null) {
            return CompletableFuture.completedFuture(cachedResult.getResult());
        }
        
        // 创建新的Future
        CompletableFuture<TaskResult<R>> newFuture = taskService.getTaskResultAsync(taskId);
        
        // 添加回调处理完成的结果
        newFuture.thenAccept(result -> {
            if (result != null && result.isCompleted()) {
                // 保存到缓存
                completedTasks.put(taskId, new CompletedTaskInfo<>(result));
                // 从进行中的Future中移除
                taskFutures.remove(taskId);
            }
        });
        
        // 保存Future
        taskFutures.put(taskId, newFuture);
        
        return newFuture;
    }
    
    /**
     * 取消任务
     * @param taskId 任务ID
     * @return 是否成功取消
     */
    public boolean cancelTask(String taskId) {
        boolean cancelled = taskService.cancelTask(taskId);
        
        // 从缓存中移除
        if (cancelled) {
            completedTasks.remove(taskId);
            
            // 取消Future
            CompletableFuture<?> future = taskFutures.remove(taskId);
            if (future != null && !future.isDone()) {
                future.cancel(true);
            }
        }
        
        return cancelled;
    }
    
    /**
     * 等待任务结果
     * @param taskId 任务ID
     * @param timeoutSeconds 超时时间（秒）
     * @param <R> 结果数据类型
     * @return 任务结果
     * @throws InterruptedException 如果线程被中断
     * @throws ExecutionException 如果任务执行出错
     * @throws TimeoutException 如果任务执行超时
     */
    public <R> TaskResult<R> waitForResult(String taskId, long timeoutSeconds) 
            throws InterruptedException, ExecutionException, TimeoutException {
        
        CompletableFuture<TaskResult<R>> future = getTaskResultAsync(taskId);
        return future.get(timeoutSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * 清理已完成任务的缓存
     */
    private void cleanupCompletedTasks() {
        try {
            logger.debug("开始清理已完成任务缓存，当前缓存大小: {}", completedTasks.size());
            
            // 计算过期时间（1小时前）
            long expiryTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
            
            // 移除过期的已完成任务
            completedTasks.entrySet().removeIf(entry -> 
                entry.getValue().getTimestamp() < expiryTime
            );
            
            // 重置计数器
            taskCounter.set(Math.max(completedTasks.size(), taskFutures.size()));
            
            logger.debug("清理完成，剩余缓存大小: {}", completedTasks.size());
        } catch (Exception e) {
            logger.error("清理缓存失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 生成任务ID
     * @return 生成的任务ID
     */
    private String generateTaskId() {
        return "task-" + UUID.randomUUID().toString().replace("-", "");
    }
    
    /**
     * 已完成任务信息，用于本地缓存
     */
    private static class CompletedTaskInfo<R> {
        private final TaskResult<R> result;
        private final long timestamp;
        
        public CompletedTaskInfo(TaskResult<R> result) {
            this.result = result;
            this.timestamp = System.currentTimeMillis();
        }
        
        public TaskResult<R> getResult() {
            return result;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
    }
} 