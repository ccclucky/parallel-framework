package pers.cclucky.parallel.core.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.api.Task;
import pers.cclucky.parallel.api.TaskContext;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.api.TaskSlice;
import pers.cclucky.parallel.core.storage.TaskStorage;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 本地Worker实现，负责执行分配给当前节点的任务分片
 */
public class LocalWorker implements Worker {
    private static final Logger logger = LoggerFactory.getLogger(LocalWorker.class);
    
    private final String nodeId;
    private final TaskStorage taskStorage;
    private final ExecutorService executorService;
    private final long pollingInterval;
    private final AtomicBoolean running;
    private ScheduledExecutorService pollingExecutor;
    
    // 用于跟踪处理过的分片，避免重复处理
    private final ConcurrentMap<String, Long> processedSlices;
    // 本地内存缓存的最大处理分片数
    private static final int MAX_PROCESSED_SLICES_CACHE = 1000;
    // 缓存清理间隔（秒）
    private static final int CACHE_CLEANUP_INTERVAL = 300;
    
    public LocalWorker(String nodeId, TaskStorage taskStorage) {
        this.nodeId = nodeId;
        this.taskStorage = taskStorage;
        this.running = new AtomicBoolean(false);
        this.pollingInterval = 1000; // 默认1秒轮询一次
        this.processedSlices = new ConcurrentHashMap<>();
        
        // 创建执行任务的线程池
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        this.executorService = new ThreadPoolExecutor(
                corePoolSize,
                corePoolSize * 2,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(200),
                r -> {
                    Thread t = new Thread(r, "worker-executor-" + nodeId);
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
    
    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("启动Worker: {}", nodeId);
            
            // 启动轮询任务
            pollingExecutor = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "worker-scheduler-" + nodeId);
                t.setDaemon(true);
                return t;
            });
            
            // 启动定期任务轮询
            pollingExecutor.scheduleAtFixedRate(this::pollTasks, 0, pollingInterval, TimeUnit.MILLISECONDS);
            
            // 启动缓存清理任务
            pollingExecutor.scheduleAtFixedRate(this::cleanupProcessedSlicesCache, 
                    CACHE_CLEANUP_INTERVAL, CACHE_CLEANUP_INTERVAL, TimeUnit.SECONDS);
        }
    }
    
    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("停止Worker: {}", nodeId);
            
            // 停止轮询任务
            if (pollingExecutor != null) {
                pollingExecutor.shutdown();
                try {
                    if (!pollingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        pollingExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    pollingExecutor.shutdownNow();
                }
            }
            
            // 停止执行任务的线程池
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executorService.shutdownNow();
            }
            
            // 清理本地缓存
            processedSlices.clear();
        }
    }
    
    @Override
    public CompletableFuture<Boolean> executeSlice(String taskId, String sliceId) {
        // 检查是否已经处理过此分片
        String sliceKey = taskId + ":" + sliceId;
        if (processedSlices.containsKey(sliceKey)) {
            logger.debug("跳过已处理的分片: taskId={}, sliceId={}", taskId, sliceId);
            return CompletableFuture.completedFuture(true);
        }
        
        logger.info("执行任务分片: taskId={}, sliceId={}", taskId, sliceId);
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // 获取分片信息
                TaskSlice<?> slice = getTaskSlice(taskId, sliceId);
                if (slice == null) {
                    logger.error("获取分片失败: taskId={}, sliceId={}", taskId, sliceId);
                    return false;
                }
                
                // 获取任务元数据
                Map<String, Object> metadata = taskStorage.getTaskMetadata(taskId);
                String taskClassName = (String) metadata.get("taskClass");
                
                // 实例化任务
                Class<?> taskClass = Class.forName(taskClassName);
                Task<?, ?> task = (Task<?, ?>) taskClass.newInstance();
                
                // 创建任务上下文
                TaskContext context = new TaskContext(taskId);
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    context.setParameter(entry.getKey(), entry.getValue());
                }
                
                // 执行分片
                Object result = processSlice(task, slice, context);
                
                // 保存分片结果
                saveSliceResult(taskId, sliceId, result, slice.getIndex());
                
                // 标记为已处理
                processedSlices.put(sliceKey, System.currentTimeMillis());
                
                // 主动请求GC回收大对象
                task = null;
                slice = null;
                context = null;
                result = null;
                
                return true;
            } catch (Exception e) {
                logger.error("执行分片失败: taskId={}, sliceId={}, error={}", 
                        taskId, sliceId, e.getMessage(), e);
                
                // 保存失败结果
                saveSliceFailure(taskId, sliceId, e);
                
                return false;
            }
        }, executorService);
    }
    
    /**
     * 轮询任务
     */
    private void pollTasks() {
        if (!running.get()) {
            return;
        }
        
        try {
            // 获取分配给当前Worker的所有分片
            List<Map<String, String>> workerSlices = taskStorage.getWorkerSlices(nodeId);
            if (workerSlices == null || workerSlices.isEmpty()) {
                return;
            }
            
            for (Map<String, String> sliceInfo : workerSlices) {
                String taskId = sliceInfo.get("taskId");
                String sliceId = sliceInfo.get("sliceId");
                
                // 检查分片是否已经执行
                TaskResult<?> result = taskStorage.getSliceResult(taskId, sliceId);
                if (result == null || !result.isCompleted()) {
                    // 执行分片
                    executeSlice(taskId, sliceId);
                } else {
                    // 分片已处理完，从Worker的任务列表中移除
                    String sliceKey = taskId + ":" + sliceId;
                    if (!processedSlices.containsKey(sliceKey)) {
                        processedSlices.put(sliceKey, System.currentTimeMillis());
                    }
                    
                    // 清理工作节点对分片的引用
                    taskStorage.cleanupWorkerSlices(nodeId, sliceKey);
                }
            }
        } catch (Exception e) {
            logger.error("轮询任务失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 清理本地缓存中的已处理分片记录
     */
    private void cleanupProcessedSlicesCache() {
        try {
            if (processedSlices.size() <= MAX_PROCESSED_SLICES_CACHE) {
                return;
            }
            
            // 计算清理的时间阈值（默认1小时前的记录可以清理）
            long expiryThreshold = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
            
            // 遍历缓存，删除过期记录
            processedSlices.entrySet().removeIf(entry -> 
                entry.getValue() < expiryThreshold
            );
            
            // 如果仍然超过最大缓存限制，强制清理最老的记录
            if (processedSlices.size() > MAX_PROCESSED_SLICES_CACHE) {
                // 找出最老的记录
                List<Map.Entry<String, Long>> entries = new java.util.ArrayList<>(processedSlices.entrySet());
                entries.sort(Map.Entry.comparingByValue());
                
                // 计算需要删除的数量
                int removeCount = entries.size() - MAX_PROCESSED_SLICES_CACHE;
                
                // 删除最老的记录
                for (int i = 0; i < removeCount; i++) {
                    processedSlices.remove(entries.get(i).getKey());
                }
            }
            
            logger.debug("清理本地缓存完成，当前缓存大小: {}", processedSlices.size());
        } catch (Exception e) {
            logger.error("清理本地缓存失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 获取任务分片
     */
    @SuppressWarnings("unchecked")
    private <T> TaskSlice<T> getTaskSlice(String taskId, String sliceId) {
        try {
            List<TaskSlice<Object>> slices = taskStorage.getTaskSlices(taskId);
            for (TaskSlice<?> slice : slices) {
                if (slice.getSliceId().equals(sliceId)) {
                    return (TaskSlice<T>) slice;
                }
            }
            return null;
        } catch (Exception e) {
            logger.error("获取任务分片失败: taskId={}, sliceId={}", taskId, sliceId, e);
            return null;
        }
    }
    
    /**
     * 处理分片
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T, R> R processSlice(Task task, TaskSlice<T> slice, TaskContext context) {
        return (R) task.process(slice, context);
    }
    
    /**
     * 保存分片结果
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private <R> void saveSliceResult(String taskId, String sliceId, R result, int sliceIndex) {
        TaskResult<R> taskResult = new TaskResult.Builder<R>()
                .taskId(taskId)
                .sliceId(sliceId)
                .status(TaskResult.Status.SUCCESS)
                .data(result)
                .sliceIndex(sliceIndex)
                .build();
        taskStorage.saveSliceResult(taskId, sliceId, taskResult);
    }
    
    /**
     * 保存分片失败结果
     */
    private void saveSliceFailure(String taskId, String sliceId, Exception e) {
        TaskResult<?> taskResult = TaskResult.failed(sliceId, e);
        taskStorage.saveSliceResult(taskId, sliceId, taskResult);
    }
}
