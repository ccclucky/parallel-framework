package pers.cclucky.parallel.core.schedule;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.api.Task;
import pers.cclucky.parallel.api.TaskContext;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.api.TaskSlice;
import pers.cclucky.parallel.core.election.MasterChangeAdapter;
import pers.cclucky.parallel.core.election.MasterElectionService;
import pers.cclucky.parallel.core.loadbalance.LoadBalancer;
import pers.cclucky.parallel.core.loadbalance.ConsistentHashLoadBalancer;
import pers.cclucky.parallel.core.storage.TaskStorage;
import pers.cclucky.parallel.core.heartbeat.HeartbeatDetector;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 分布式任务调度器实现
 * 负责任务的分片、分发和结果收集，支持高可用和数据一致性
 */
public class DistributedTaskScheduler implements TaskScheduler {
    private static final Logger logger = LoggerFactory.getLogger(DistributedTaskScheduler.class);
    
    private static final String TASK_LOCK_PREFIX = "lock:task:";
    private static final int DEFAULT_SLICE_SIZE = 100;
    private static final long TASK_CHECK_INTERVAL = 5000; // ms
    private static final long TASK_LOCK_TIMEOUT = 30; // seconds
    private static final int MAX_RETRIES = 3;
    private static final long CLEANUP_INTERVAL = 1800000; // 30分钟
    
    private final String nodeId;
    private final TaskStorage taskStorage;
    private final RedissonClient redissonClient;
    private final MasterElectionService masterElectionService;
    private final HeartbeatDetector heartbeatDetector;
    private final LoadBalancer loadBalancer;
    private final ExecutorService taskExecutor;
    private final ScheduledExecutorService schedulerExecutor;
    private final Map<String, CompletableFuture<? extends TaskResult<?>>> resultFutures;
    private final AtomicBoolean running;
    
    /**
     * 创建分布式任务调度器
     * @param nodeId 节点ID
     * @param taskStorage 任务存储
     * @param redissonClient Redis客户端
     * @param masterElectionService 主节点选举服务
     * @param heartbeatDetector 心跳检测器
     */
    public DistributedTaskScheduler(String nodeId, TaskStorage taskStorage, RedissonClient redissonClient, 
                                     MasterElectionService masterElectionService,
                                     HeartbeatDetector heartbeatDetector) {
        this.nodeId = nodeId;
        this.taskStorage = taskStorage;
        this.redissonClient = redissonClient;
        this.masterElectionService = masterElectionService;
        this.heartbeatDetector = heartbeatDetector;
        this.loadBalancer = new ConsistentHashLoadBalancer();
        
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        this.taskExecutor = new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize * 2,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> {
                    Thread t = new Thread(r, "task-executor-" + UUID.randomUUID().toString().substring(0, 8));
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        
        this.schedulerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "task-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        this.resultFutures = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
    }
    
    @Override
    public <T, R> String submitTask(Task<T, R> task) {
        return submitTask(task, new HashMap<>());
    }
    
    @Override
    public <T, R> String submitTask(Task<T, R> task, Map<String, Object> params) {
        String taskId = generateTaskId();
        logger.info("提交任务: taskId={}, taskClass={}", taskId, task.getClass().getName());
        
        try {
            // 初始化任务
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("taskClass", task.getClass().getName());
            metadata.put("submitTime", System.currentTimeMillis());
            metadata.put("submitNode", nodeId);
            metadata.putAll(params);
            
            // 保存任务状态
            taskStorage.saveTaskStatus(taskId, "PENDING", metadata);
            
            // 如果当前节点是Master，则立即调度任务
            if (masterElectionService.isMaster()) {
                scheduleTask(taskId, task, params);
            }
            
            return taskId;
        } catch (Exception e) {
            logger.error("提交任务失败: taskId={}, error={}", taskId, e.getMessage(), e);
            taskStorage.saveTaskStatus(taskId, "FAILED", Collections.singletonMap("error", e.getMessage()));
            throw new RuntimeException("提交任务失败", e);
        }
    }
    
    @Override
    public <T, R> CompletableFuture<String> submitTaskAsync(Task<T, R> task) {
        return submitTaskAsync(task, new HashMap<>());
    }
    
    @Override
    public <T, R> CompletableFuture<String> submitTaskAsync(Task<T, R> task, Map<String, Object> params) {
        return CompletableFuture.supplyAsync(() -> submitTask(task, params), taskExecutor);
    }
    
    @Override
    public String getTaskStatus(String taskId) {
        return taskStorage.getTaskStatus(taskId);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <R> TaskResult<R> getTaskResult(String taskId) {
        return taskStorage.getTaskResult(taskId);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <R> CompletableFuture<TaskResult<R>> getTaskResultAsync(String taskId) {
        // 使用泛型通配符避免类型转换问题
        CompletableFuture<? extends TaskResult<?>> existingFuture = resultFutures.get(taskId);
        
        if (existingFuture == null) {
            CompletableFuture<TaskResult<R>> future = new CompletableFuture<>();
            
            // 检查任务是否已完成
            TaskResult<R> result = getTaskResult(taskId);
            if (result != null) {
                future.complete(result);
                return future;
            }
            
            // 注册未完成任务的Future
            resultFutures.put(taskId, future);
            
            // 轮询检查任务结果
            schedulerExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    TaskResult<R> result = getTaskResult(taskId);
                    if (result != null) {
                        future.complete(result);
                        resultFutures.remove(taskId);
                    } else {
                        String status = getTaskStatus(taskId);
                        if ("FAILED".equals(status) || "CANCELLED".equals(status)) {
                            future.completeExceptionally(new RuntimeException("任务" + status));
                            resultFutures.remove(taskId);
                        } else if (running.get()) {
                            schedulerExecutor.schedule(this, 1, TimeUnit.SECONDS);
                        }
                    }
                }
            }, 500, TimeUnit.MILLISECONDS);
            
            return future;
        }
        
        // 使用unchecked cast，因为我们知道类型是兼容的
        return (CompletableFuture<TaskResult<R>>) existingFuture;
    }
    
    @Override
    public boolean cancelTask(String taskId) {
        logger.info("取消任务: taskId={}", taskId);
        
        RLock lock = redissonClient.getLock(TASK_LOCK_PREFIX + taskId);
        try {
            if (lock.tryLock(10, 30, TimeUnit.SECONDS)) {
                try {
                    String status = taskStorage.getTaskStatus(taskId);
                    if ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
                        logger.warn("取消任务失败，任务已处于终止状态: taskId={}, status={}", taskId, status);
                        return false;
                    }
                    
                    // 更新任务状态
                    taskStorage.updateTaskStatus(taskId, "CANCELLED");
                    
                    // 取消未完成的Future
                    CompletableFuture<?> future = (CompletableFuture<?>) resultFutures.remove(taskId);
                    if (future != null && !future.isDone()) {
                        future.completeExceptionally(new CancellationException("任务被取消"));
                    }
                    
                    return true;
                } finally {
                    lock.unlock();
                }
            } else {
                logger.warn("获取任务锁超时: taskId={}", taskId);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("取消任务被中断: taskId={}", taskId, e);
            return false;
        } catch (Exception e) {
            logger.error("取消任务失败: taskId={}", taskId, e);
            return false;
        }
    }
    
    @Override
    public boolean rescheduleTask(String taskId) {
        logger.info("重新调度任务: taskId={}", taskId);
        
        if (!masterElectionService.isMaster()) {
            logger.warn("当前节点不是Master，无法重新调度任务: taskId={}", taskId);
            return false;
        }
        
        RLock lock = redissonClient.getLock(TASK_LOCK_PREFIX + taskId);
        try {
            if (lock.tryLock(10, 30, TimeUnit.SECONDS)) {
                try {
                    String status = taskStorage.getTaskStatus(taskId);
                    if ("RUNNING".equals(status)) {
                        logger.warn("重新调度任务失败，任务正在运行: taskId={}", taskId);
                        return false;
                    }
                    
                    if ("PENDING".equals(status) || "FAILED".equals(status)) {
                        Map<String, Object> metadata = taskStorage.getTaskMetadata(taskId);
                        String taskClassName = (String) metadata.get("taskClass");
                        
                        try {
                            // 实例化任务
                            Class<?> taskClass = Class.forName(taskClassName);
                            Task<?, ?> task = (Task<?, ?>) taskClass.newInstance();
                            
                            // 更新状态并重新调度
                            taskStorage.updateTaskStatus(taskId, "PENDING");
                            scheduleTask(taskId, task, metadata);
                            
                            return true;
                        } catch (Exception e) {
                            logger.error("重新调度任务失败: taskId={}, error={}", taskId, e.getMessage(), e);
                            return false;
                        }
                    } else {
                        logger.warn("重新调度任务失败，任务状态不允许: taskId={}, status={}", taskId, status);
                        return false;
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                logger.warn("获取任务锁超时: taskId={}", taskId);
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("重新调度任务被中断: taskId={}", taskId, e);
            return false;
        } catch (Exception e) {
            logger.error("重新调度任务失败: taskId={}", taskId, e);
            return false;
        }
    }
    
    @Override
    public int handleWorkerFailure(String workerId) {
        logger.info("处理Worker节点失效: workerId={}", workerId);
        
        if (!masterElectionService.isMaster()) {
            logger.warn("当前节点不是Master，无法处理Worker节点失效: workerId={}", workerId);
            return 0;
        }
        
        try {
            // 获取该Worker的所有分片
            List<Map<String, String>> workerSlices = taskStorage.getWorkerSlices(workerId);
            if (workerSlices.isEmpty()) {
                logger.info("Worker节点没有正在处理的分片: workerId={}", workerId);
                return 0;
            }
            
            int reassignedCount = 0;
            
            // 查找活跃的Worker节点
            List<String> activeWorkers = new ArrayList<>();
            // TODO: 从心跳检测器获取活跃Worker列表
            
            if (activeWorkers.isEmpty()) {
                logger.warn("没有活跃的Worker节点可用，无法重新分配任务");
                return 0;
            }
            
            // 重新分配分片
            for (Map<String, String> sliceInfo : workerSlices) {
                String taskId = sliceInfo.get("taskId");
                String sliceId = sliceInfo.get("sliceId");
                
                RLock lock = redissonClient.getLock(TASK_LOCK_PREFIX + taskId);
                try {
                    if (lock.tryLock(5, 10, TimeUnit.SECONDS)) {
                        try {
                            // 选择新的Worker
                            String newWorkerId = loadBalancer.selectNode(activeWorkers, taskId + ":" + sliceId);
                            
                            // 重新分配分片
                            if (taskStorage.assignSliceToWorker(taskId, sliceId, newWorkerId)) {
                                reassignedCount++;
                                logger.info("重新分配分片: taskId={}, sliceId={}, from={}, to={}", 
                                        taskId, sliceId, workerId, newWorkerId);
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("重新分配分片被中断: taskId={}, sliceId={}", taskId, sliceId, e);
                } catch (Exception e) {
                    logger.error("重新分配分片失败: taskId={}, sliceId={}, error={}", 
                            taskId, sliceId, e.getMessage(), e);
                }
            }
            
            logger.info("Worker节点失效处理完成: workerId={}, 重新分配分片数量={}", workerId, reassignedCount);
            return reassignedCount;
        } catch (Exception e) {
            logger.error("处理Worker节点失效失败: workerId={}, error={}", workerId, e.getMessage(), e);
            return 0;
        }
    }
    
    @Override
    public int getTaskProgress(String taskId) {
        return taskStorage.getTaskProgress(taskId);
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <R> TaskResult<R> getSliceResult(String taskId, String sliceId) {
        return taskStorage.getSliceResult(taskId, sliceId);
    }
    
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> List<TaskSlice<T>> getPendingSlices(String taskId) {
        try {
            // 获取所有分片，这里要做一次类型转换（Object -> 实际类型）
            List<TaskSlice<Object>> slices = taskStorage.getTaskSlices(taskId);
            if (slices.isEmpty()) {
                return Collections.emptyList();
            }
            
            // 过滤出未完成的分片
            List<TaskSlice<Object>> pendingSlices = new ArrayList<>();
            for (TaskSlice<?> slice : slices) {
                TaskResult<?> result = taskStorage.getSliceResult(taskId, slice.getSliceId());
                if (result == null || !result.isCompleted()) {
                    pendingSlices.add((TaskSlice<Object>) slice);
                }
            }
            
            // 进行安全的类型转换
            return (List<TaskSlice<T>>) (List) pendingSlices;
        } catch (Exception e) {
            logger.error("获取未完成分片失败: taskId={}", taskId, e);
            return Collections.emptyList();
        }
    }
    
    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("启动分布式任务调度器");
            
            // 注册Master变更监听器，在成为Master时处理未完成的任务
            masterElectionService.registerMasterChangeListener(new MasterChangeAdapter() {
                @Override
                public void onBecomeMaster(String masterId) {
                    if (nodeId.equals(masterId)) {
                        handlePendingTasks();
                    }
                }
                
                @Override
                public void onMasterChange(String oldMasterId, String newMasterId) {
                    if (nodeId.equals(newMasterId)) {
                        handlePendingTasks();
                    }
                }
            });
            
            // 启动任务检查
            startTaskChecker();
            
            // 启动定期清理过期任务的任务
            startCleanupTask();
            
            logger.info("分布式任务调度器启动完成");
        }
    }
    
    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("停止分布式任务调度器");
            
            // 停止调度器
            schedulerExecutor.shutdown();
            try {
                if (!schedulerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    schedulerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                schedulerExecutor.shutdownNow();
            }
            
            // 停止任务执行器
            taskExecutor.shutdown();
            try {
                if (!taskExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    taskExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                taskExecutor.shutdownNow();
            }
            
            // 取消所有未完成的Future
            for (Map.Entry<String, CompletableFuture<? extends TaskResult<?>>> entry : resultFutures.entrySet()) {
                CompletableFuture<?> future = entry.getValue();
                if (!future.isDone()) {
                    future.completeExceptionally(new CancellationException("调度器已停止"));
                }
            }
            resultFutures.clear();
            
            logger.info("分布式任务调度器已停止");
        }
    }
    
    /**
     * 成为Master时的处理
     */
    private void handlePendingTasks() {
        logger.info("节点成为Master，处理未完成的任务");
        
        try {
            // 获取所有运行中的任务
            List<String> runningTasks = taskStorage.getRunningTasks();
            logger.info("发现运行中的任务数量: {}", runningTasks.size());
            
            // 处理每个任务
            for (String taskId : runningTasks) {
                try {
                    // 检查任务状态是否需要重新调度
                    List<TaskSlice<Object>> pendingSlices = getPendingSlices(taskId);
                    if (!pendingSlices.isEmpty()) {
                        logger.info("任务有未完成的分片: taskId={}, pendingSlices={}", taskId, pendingSlices.size());
                        rescheduleTask(taskId);
                    }
                } catch (Exception e) {
                    logger.error("处理未完成任务失败: taskId={}, error={}", taskId, e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error("处理未完成任务失败: error={}", e.getMessage(), e);
        }
    }
    
    /**
     * 启动任务检查器
     */
    private void startTaskChecker() {
        schedulerExecutor.scheduleAtFixedRate(() -> {
            if (!masterElectionService.isMaster() || !running.get()) {
                return;
            }
            
            try {
                // 获取所有运行中的任务
                List<String> runningTasks = taskStorage.getRunningTasks();
                
                for (String taskId : runningTasks) {
                    checkTaskStatus(taskId);
                }
            } catch (Exception e) {
                logger.error("任务检查失败: error={}", e.getMessage(), e);
            }
        }, TASK_CHECK_INTERVAL, TASK_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 启动定期清理过期任务的任务
     */
    private void startCleanupTask() {
        schedulerExecutor.scheduleAtFixedRate(() -> {
            if (!masterElectionService.isMaster() || !running.get()) {
                return;
            }
            
            try {
                int cleanedCount = taskStorage.cleanupExpiredTasks();
                if (cleanedCount > 0) {
                    logger.info("已清理{}个过期任务", cleanedCount);
                }
            } catch (Exception e) {
                logger.error("清理过期任务失败: error={}", e.getMessage(), e);
            }
        }, CLEANUP_INTERVAL, CLEANUP_INTERVAL, TimeUnit.MILLISECONDS);
    }
    
    /**
     * 检查任务状态
     * @param taskId 任务ID
     */
    private void checkTaskStatus(String taskId) {
        RLock lock = redissonClient.getLock(TASK_LOCK_PREFIX + taskId);
        try {
            if (lock.tryLock(5, 10, TimeUnit.SECONDS)) {
                try {
                    // 获取任务状态
                    String status = taskStorage.getTaskStatus(taskId);
                    if (!"RUNNING".equals(status)) {
                        return;
                    }
                    
                    // 获取所有分片
                    List<TaskSlice<Object>> allSlices = taskStorage.getTaskSlices(taskId);
                    if (allSlices.isEmpty()) {
                        return;
                    }
                    
                    // 检查是否所有分片都已完成
                    boolean allCompleted = true;
                    int completedCount = 0;
                    
                    for (TaskSlice<?> slice : allSlices) {
                        TaskResult<?> result = taskStorage.getSliceResult(taskId, slice.getSliceId());
                        if (result != null && result.isCompleted()) {
                            completedCount++;
                        } else {
                            allCompleted = false;
                        }
                    }
                    
                    // 更新任务进度
                    int progress = (int) ((float) completedCount / allSlices.size() * 100);
                    taskStorage.saveTaskProgress(taskId, progress, "已完成 " + completedCount + "/" + allSlices.size() + " 个分片");
                    
                    // 如果所有分片已完成，合并结果
                    if (allCompleted) {
                        completeTask(taskId);
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("检查任务状态被中断: taskId={}", taskId, e);
        } catch (Exception e) {
            logger.error("检查任务状态失败: taskId={}, error={}", taskId, e.getMessage(), e);
        }
    }
    
    /**
     * 完成任务，合并分片结果
     * @param taskId 任务ID
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> void completeTask(String taskId) {
        logger.info("完成任务: taskId={}", taskId);
        
        try {
            // 获取任务元数据
            Map<String, Object> metadata = taskStorage.getTaskMetadata(taskId);
            String taskClassName = (String) metadata.get("taskClass");
            
            // 实例化任务
            Class<?> taskClass = Class.forName(taskClassName);
            Task<?, R> task = (Task<?, R>) taskClass.newInstance();
            
            // 获取所有分片结果
            List<TaskResult<R>> sliceResults = (List<TaskResult<R>>) (List) taskStorage.getAllSliceResults(taskId);
            
            // 检查是否有失败的分片
            boolean hasFailure = sliceResults.stream().anyMatch(r -> !r.isSuccess());
            
            // 创建任务结果
            TaskResult<R> taskResult;
            if (hasFailure) {
                // 如果有失败的分片，标记任务失败
                String errorMessages = sliceResults.stream()
                        .filter(r -> !r.isSuccess())
                        .map(TaskResult::getErrorString)
                        .collect(Collectors.joining("; "));
                
                taskResult = TaskResult.failed(taskId, new RuntimeException("部分分片执行失败: " + errorMessages));
            } else {
                // 提取所有成功的结果值并按索引排序
                List<R> resultValues = sliceResults.stream()
                        .sorted(Comparator.comparing(TaskResult::getSliceIndex))
                        .map(TaskResult::getResult)
                        .collect(Collectors.toList());
                
                // 创建任务上下文
                TaskContext context = new TaskContext(taskId);
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    context.setParameter(entry.getKey(), entry.getValue());
                }
                
                // 调用task.reduce()合并结果
                R finalResult = task.reduce(resultValues, context);
                
                // 使用合并后的结果创建任务结果
                taskResult = TaskResult.success(taskId, finalResult);
            }
            
            // 保存任务结果
            taskStorage.saveTaskResult(taskId, taskResult);
            
            // 更新任务状态
            taskStorage.updateTaskStatus(taskId, taskResult.isSuccess() ? "COMPLETED" : "FAILED");
            
            // 完成Future
            CompletableFuture<TaskResult<?>> future = (CompletableFuture<TaskResult<?>>) resultFutures.remove(taskId);
            if (future != null && !future.isDone()) {
                future.complete(taskResult);
            }
            
            // 任务完成后，清理分片数据以节省内存
            if (taskResult.isSuccess()) {
                // 任务成功完成后，异步清理分片数据
                taskExecutor.submit(() -> {
                    try {
                        // 给客户端一些时间获取结果后再清理
                        TimeUnit.SECONDS.sleep(30);
                        
                        if (taskStorage.cleanupTaskSlices(taskId)) {
                            logger.info("成功清理任务分片数据: taskId={}", taskId);
                        }
                    } catch (Exception e) {
                        logger.error("清理任务分片数据失败: taskId={}, error={}", taskId, e.getMessage(), e);
                    }
                });
            }
        } catch (Exception e) {
            logger.error("合并任务结果失败: taskId={}, error={}", taskId, e.getMessage(), e);
            
            // 标记任务失败
            TaskResult<R> taskResult = TaskResult.failed(taskId, new RuntimeException("合并任务结果失败: " + e.getMessage()));
            taskStorage.saveTaskResult(taskId, taskResult);
            taskStorage.updateTaskStatus(taskId, "FAILED");
            
            // 完成Future（异常）
            CompletableFuture<TaskResult<?>> future = (CompletableFuture<TaskResult<?>>) resultFutures.remove(taskId);
            if (future != null && !future.isDone()) {
                future.completeExceptionally(e);
            }
        }
    }
    
    /**
     * 调度任务
     * @param taskId 任务ID
     * @param task 任务对象
     * @param params 任务参数
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T, R> void scheduleTask(String taskId, Task<T, R> task, Map<String, Object> params) {
        logger.info("调度任务: taskId={}", taskId);
        
        if (!masterElectionService.isMaster()) {
            logger.warn("当前节点不是Master，无法调度任务: taskId={}", taskId);
            return;
        }
        
        RLock lock = redissonClient.getLock(TASK_LOCK_PREFIX + taskId);
        try {
            if (lock.tryLock(10, TASK_LOCK_TIMEOUT, TimeUnit.SECONDS)) {
                try {
                    // 更新任务状态为运行中
                    taskStorage.updateTaskStatus(taskId, "RUNNING");
                    
                    // 创建任务上下文
                    TaskContext context = new TaskContext(taskId);
                    
                    // 添加参数
                    for (Map.Entry<String, Object> entry : params.entrySet()) {
                        context.setParameter(entry.getKey(), entry.getValue());
                    }
                    
                    // 使用Task的slice方法进行分片
                    List<TaskSlice<T>> slices = task.slice(context);
                    
                    if (slices == null || slices.isEmpty()) {
                        logger.warn("任务分片为空: taskId={}", taskId);
                        TaskResult<R> emptyResult = TaskResult.success(taskId, null);
                        taskStorage.saveTaskResult(taskId, emptyResult);
                        taskStorage.updateTaskStatus(taskId, "COMPLETED");
                        
                        CompletableFuture<TaskResult<?>> future = (CompletableFuture<TaskResult<?>>) resultFutures.remove(taskId);
                        if (future != null && !future.isDone()) {
                            future.complete(emptyResult);
                        }
                        
                        return;
                    }
                    
                    // 保存分片信息
                    taskStorage.saveTaskSlices(taskId, slices);
                    
                    // 获取活跃的Worker列表
                    List<String> availableWorkers = new ArrayList<>(heartbeatDetector.getAliveWorkers());
                    
                    if (availableWorkers.isEmpty()) {
                        logger.warn("没有发现活跃的Worker节点，使用当前节点执行任务: {}", nodeId);
                        availableWorkers.add(nodeId);
                    }
                    
                    // 分配分片到Worker
                    for (TaskSlice<T> slice : slices) {
                        String sliceId = slice.getSliceId();
                        String workerId = loadBalancer.selectNode(availableWorkers, taskId + ":" + sliceId);
                        
                        // 分配分片
                        taskStorage.assignSliceToWorker(taskId, sliceId, workerId);
                    }
                    
                    // 更新任务进度
                    taskStorage.saveTaskProgress(taskId, 0, "任务已分片，等待执行");
                    
                    logger.info("任务调度完成: taskId={}, totalSlices={}", taskId, slices.size());
                } finally {
                    lock.unlock();
                }
            } else {
                logger.warn("获取任务锁超时: taskId={}", taskId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("任务调度被中断: taskId={}", taskId, e);
            taskStorage.updateTaskStatus(taskId, "FAILED");
        } catch (Exception e) {
            logger.error("任务调度失败: taskId={}, error={}", taskId, e.getMessage(), e);
            taskStorage.updateTaskStatus(taskId, "FAILED");
            
            // 设置任务结果
            TaskResult<R> failureResult = TaskResult.failed(taskId, new RuntimeException("任务调度失败: " + e.getMessage()));
            taskStorage.saveTaskResult(taskId, failureResult);
            
            // 完成Future（异常）
            CompletableFuture<TaskResult<?>> future = (CompletableFuture<TaskResult<?>>) resultFutures.remove(taskId);
            if (future != null && !future.isDone()) {
                future.completeExceptionally(e);
            }
        }
    }
    
    /**
     * 生成任务ID
     * @return 任务ID
     */
    private String generateTaskId() {
        return "task_" + UUID.randomUUID().toString().replace("-", "");
    }
} 