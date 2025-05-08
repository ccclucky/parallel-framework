package pers.cclucky.parallel.core.storage;

import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RLock;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.api.TaskSlice;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 基于Redis的任务存储实现
 */
public class RedisTaskStorage implements TaskStorage {
    private static final Logger logger = LoggerFactory.getLogger(RedisTaskStorage.class);
    
    private static final String TASK_STATUS_KEY = "parallel:task:status:";
    private static final String TASK_METADATA_KEY = "parallel:task:metadata:";
    private static final String TASK_SLICES_KEY = "parallel:task:slices:";
    private static final String SLICE_RESULT_KEY = "parallel:slice:result:";
    private static final String TASK_RESULT_KEY = "parallel:task:result:";
    private static final String TASK_PROGRESS_KEY = "parallel:task:progress:";
    private static final String SLICE_WORKER_KEY = "parallel:slice:worker:";
    private static final String WORKER_SLICES_KEY = "parallel:worker:slices:";
    private static final String RUNNING_TASKS_KEY = "parallel:tasks:running";
    private static final String WORKER_HEARTBEAT_KEY = "parallel:worker:heartbeat:";
    private static final String COMPLETED_TASKS_KEY = "parallel:tasks:completed";
    private static final String TASK_EXPIRY_KEY = "parallel:task:expiry:";
    
    // 任务状态过期时间（天）
    private static final long TASK_STATUS_EXPIRE_DAYS = 7;
    // 任务结果过期时间（天）
    private static final long TASK_RESULT_EXPIRE_DAYS = 7;
    // 分片结果过期时间（天）
    private static final long SLICE_RESULT_EXPIRE_DAYS = 1;
    // 心跳信息过期时间（秒）
    private static final long HEARTBEAT_EXPIRE_SECONDS = 30;
    // 默认的任务数据过期时间（小时）
    private static final long DEFAULT_TASK_EXPIRY_HOURS = 24;
    
    private final RedissonClient redissonClient;
    
    public RedisTaskStorage(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }
    
    @Override
    public boolean saveTaskStatus(String taskId, String status, Map<String, Object> metadata) {
        try {
            // 保存任务状态
            RBucket<String> bucket = redissonClient.getBucket(TASK_STATUS_KEY + taskId);
            bucket.set(status, TASK_STATUS_EXPIRE_DAYS, TimeUnit.DAYS);
            
            // 保存任务元数据
            if (metadata != null && !metadata.isEmpty()) {
                RMap<String, Object> hash = redissonClient.getMap(TASK_METADATA_KEY + taskId);
                hash.putAll(metadata);
                hash.expire(TASK_STATUS_EXPIRE_DAYS, TimeUnit.DAYS);
            }
            
            // 如果是运行中状态，添加到运行中任务集合
            RSet<String> runningTasks = redissonClient.getSet(RUNNING_TASKS_KEY);
            if ("RUNNING".equals(status)) {
                runningTasks.add(taskId);
            } else if ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
                // 如果是终止状态，从运行中任务集合移除
                runningTasks.remove(taskId);
                
                // 添加到已完成任务集合（用于后续清理）
                if ("COMPLETED".equals(status)) {
                    RSet<String> completedTasks = redissonClient.getSet(COMPLETED_TASKS_KEY);
                    completedTasks.add(taskId);
                    
                    // 设置默认过期时间
                    setTaskDataExpiry(taskId, DEFAULT_TASK_EXPIRY_HOURS * 3600);
                }
            }
            
            return true;
        } catch (Exception e) {
            logger.error("保存任务状态失败: taskId={}, status={}", taskId, status, e);
            return false;
        }
    }
    
    @Override
    public boolean updateTaskStatus(String taskId, String status) {
        try {
            RBucket<String> bucket = redissonClient.getBucket(TASK_STATUS_KEY + taskId);
            bucket.set(status, TASK_STATUS_EXPIRE_DAYS, TimeUnit.DAYS);
            
            // 更新运行中任务集合
            RSet<String> runningTasks = redissonClient.getSet(RUNNING_TASKS_KEY);
            if ("RUNNING".equals(status)) {
                runningTasks.add(taskId);
            } else if ("COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status)) {
                runningTasks.remove(taskId);
                
                // 添加到已完成任务集合（用于后续清理）
                if ("COMPLETED".equals(status)) {
                    RSet<String> completedTasks = redissonClient.getSet(COMPLETED_TASKS_KEY);
                    completedTasks.add(taskId);
                    
                    // 设置默认过期时间
                    setTaskDataExpiry(taskId, DEFAULT_TASK_EXPIRY_HOURS * 3600);
                }
            }
            
            return true;
        } catch (Exception e) {
            logger.error("更新任务状态失败: taskId={}, status={}", taskId, status, e);
            return false;
        }
    }
    
    @Override
    public String getTaskStatus(String taskId) {
        try {
            RBucket<String> bucket = redissonClient.getBucket(TASK_STATUS_KEY + taskId);
            return bucket.get();
        } catch (Exception e) {
            logger.error("获取任务状态失败: taskId={}", taskId, e);
            return null;
        }
    }
    
    @Override
    public Map<String, Object> getTaskMetadata(String taskId) {
        try {
            RMap<String, Object> hash = redissonClient.getMap(TASK_METADATA_KEY + taskId);
            return new HashMap<>(hash.readAllMap());
        } catch (Exception e) {
            logger.error("获取任务元数据失败: taskId={}", taskId, e);
            return new HashMap<>();
        }
    }
    
    @Override
    public <T> boolean saveTaskSlices(String taskId, List<TaskSlice<T>> slices) {
        if (slices == null || slices.isEmpty()) {
            return false;
        }
        
        try {
            // 使用分布式锁确保操作原子性
            RLock lock = redissonClient.getLock("lock:task:slices:" + taskId);
            try {
                if (lock.tryLock(10, 30, TimeUnit.SECONDS)) {
                    try {
                        // 清除旧数据
                        redissonClient.getKeys().delete(TASK_SLICES_KEY + taskId);
                        
                        // 保存分片信息
                        RMap<String, TaskSlice<T>> hash = redissonClient.getMap(TASK_SLICES_KEY + taskId);
                        Map<String, TaskSlice<T>> sliceMap = new HashMap<>();
                        for (TaskSlice<T> slice : slices) {
                            sliceMap.put(slice.getSliceId(), slice);
                        }
                        
                        hash.putAll(sliceMap);
                        hash.expire(TASK_STATUS_EXPIRE_DAYS, TimeUnit.DAYS);
                        
                        return true;
                    } finally {
                        lock.unlock();
                    }
                } else {
                    logger.warn("获取分布式锁超时: taskId={}", taskId);
                    return false;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("保存任务分片被中断: taskId={}", taskId, e);
                return false;
            }
        } catch (Exception e) {
            logger.error("保存任务分片失败: taskId={}", taskId, e);
            return false;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<TaskSlice<T>> getTaskSlices(String taskId) {
        try {
            RMap<String, TaskSlice<T>> hash = redissonClient.getMap(TASK_SLICES_KEY + taskId);
            Map<String, TaskSlice<T>> entries = hash.readAllMap();
            
            if (entries != null && !entries.isEmpty()) {
                return new ArrayList<>(entries.values());
            }
            
            return new ArrayList<>();
        } catch (Exception e) {
            logger.error("获取任务分片失败: taskId={}", taskId, e);
            return new ArrayList<>();
        }
    }
    
    @Override
    public <R> boolean saveSliceResult(String taskId, String sliceId, TaskResult<R> result) {
        if (result == null) {
            return false;
        }
        
        try {
            String key = SLICE_RESULT_KEY + taskId + ":" + sliceId;
            RBucket<TaskResult<R>> bucket = redissonClient.getBucket(key);
            bucket.set(result, SLICE_RESULT_EXPIRE_DAYS, TimeUnit.DAYS);
            return true;
        } catch (Exception e) {
            logger.error("保存分片结果失败: taskId={}, sliceId={}", taskId, sliceId, e);
            return false;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <R> TaskResult<R> getSliceResult(String taskId, String sliceId) {
        try {
            String key = SLICE_RESULT_KEY + taskId + ":" + sliceId;
            RBucket<TaskResult<R>> bucket = redissonClient.getBucket(key);
            return bucket.get();
        } catch (Exception e) {
            logger.error("获取分片结果失败: taskId={}, sliceId={}", taskId, sliceId, e);
            return null;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <R> List<TaskResult<R>> getAllSliceResults(String taskId) {
        try {
            // 获取所有分片
            List<TaskSlice<?>> slices = (List<TaskSlice<?>>) (Object) getTaskSlices(taskId);
            if (slices.isEmpty()) {
                return new ArrayList<>();
            }
            
            // 获取所有分片结果
            List<TaskResult<R>> results = new ArrayList<>();
            for (TaskSlice<?> slice : slices) {
                TaskResult<R> result = getSliceResult(taskId, slice.getSliceId());
                if (result != null) {
                    results.add(result);
                }
            }
            
            return results;
        } catch (Exception e) {
            logger.error("获取所有分片结果失败: taskId={}", taskId, e);
            return new ArrayList<>();
        }
    }
    
    @Override
    public <R> boolean saveTaskResult(String taskId, TaskResult<R> result) {
        if (result == null) {
            return false;
        }
        
        try {
            RBucket<TaskResult<R>> bucket = redissonClient.getBucket(TASK_RESULT_KEY + taskId);
            bucket.set(result, TASK_RESULT_EXPIRE_DAYS, TimeUnit.DAYS);
            return true;
        } catch (Exception e) {
            logger.error("保存任务结果失败: taskId={}", taskId, e);
            return false;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <R> TaskResult<R> getTaskResult(String taskId) {
        try {
            RBucket<TaskResult<R>> bucket = redissonClient.getBucket(TASK_RESULT_KEY + taskId);
            return bucket.get();
        } catch (Exception e) {
            logger.error("获取任务结果失败: taskId={}", taskId, e);
            return null;
        }
    }
    
    @Override
    public boolean saveTaskProgress(String taskId, int progress, String message) {
        try {
            RBucket<Integer> progressBucket = redissonClient.getBucket(TASK_PROGRESS_KEY + taskId + ":progress");
            progressBucket.set(progress, TASK_STATUS_EXPIRE_DAYS, TimeUnit.DAYS);
            
            if (message != null) {
                RBucket<String> messageBucket = redissonClient.getBucket(TASK_PROGRESS_KEY + taskId + ":message");
                messageBucket.set(message, TASK_STATUS_EXPIRE_DAYS, TimeUnit.DAYS);
            }
            
            return true;
        } catch (Exception e) {
            logger.error("保存任务进度失败: taskId={}, progress={}", taskId, progress, e);
            return false;
        }
    }
    
    @Override
    public int getTaskProgress(String taskId) {
        try {
            RBucket<Integer> bucket = redissonClient.getBucket(TASK_PROGRESS_KEY + taskId + ":progress");
            Integer progress = bucket.get();
            return progress != null ? progress : 0;
        } catch (Exception e) {
            logger.error("获取任务进度失败: taskId={}", taskId, e);
            return 0;
        }
    }
    
    @Override
    public String getTaskProgressMessage(String taskId) {
        try {
            RBucket<String> bucket = redissonClient.getBucket(TASK_PROGRESS_KEY + taskId + ":message");
            return bucket.get();
        } catch (Exception e) {
            logger.error("获取任务进度消息失败: taskId={}", taskId, e);
            return null;
        }
    }
    
    @Override
    public boolean assignSliceToWorker(String taskId, String sliceId, String workerId) {
        try {
            String key = SLICE_WORKER_KEY + taskId + ":" + sliceId;
            RBucket<String> bucket = redissonClient.getBucket(key);
            bucket.set(workerId, TASK_STATUS_EXPIRE_DAYS, TimeUnit.DAYS);
            
            // 添加到工作节点任务列表
            RSet<String> workerSlices = redissonClient.getSet(WORKER_SLICES_KEY + workerId);
            workerSlices.add(taskId + ":" + sliceId);
            
            return true;
        } catch (Exception e) {
            logger.error("分配分片到工作节点失败: taskId={}, sliceId={}, workerId={}", taskId, sliceId, workerId, e);
            return false;
        }
    }
    
    @Override
    public String getSliceWorker(String taskId, String sliceId) {
        try {
            String key = SLICE_WORKER_KEY + taskId + ":" + sliceId;
            RBucket<String> bucket = redissonClient.getBucket(key);
            return bucket.get();
        } catch (Exception e) {
            logger.error("获取分片工作节点失败: taskId={}, sliceId={}", taskId, sliceId, e);
            return null;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public List<Map<String, String>> getWorkerSlices(String workerId) {
        try {
            RSet<String> workerSlices = redissonClient.getSet(WORKER_SLICES_KEY + workerId);
            if (workerSlices.isEmpty()) {
                return new ArrayList<>();
            }
            
            List<Map<String, String>> result = new ArrayList<>();
            for (String taskSlice : workerSlices) {
                String[] parts = taskSlice.split(":");
                if (parts.length == 2) {
                    Map<String, String> item = new HashMap<>();
                    item.put("taskId", parts[0]);
                    item.put("sliceId", parts[1]);
                    result.add(item);
                }
            }
            
            return result;
        } catch (Exception e) {
            logger.error("获取工作节点分片失败: workerId={}", workerId, e);
            return new ArrayList<>();
        }
    }
    
    @Override
    public boolean removeTask(String taskId) {
        try {
            // 使用分布式锁确保操作原子性
            RLock lock = redissonClient.getLock("lock:task:remove:" + taskId);
            try {
                if (lock.tryLock(10, 30, TimeUnit.SECONDS)) {
                    try {
                        // 获取该任务的所有分片
                        List<TaskSlice<?>> slices = (List<TaskSlice<?>>) (Object) getTaskSlices(taskId);
                        
                        // 删除所有分片结果
                        for (TaskSlice<?> slice : slices) {
                            String resultKey = SLICE_RESULT_KEY + taskId + ":" + slice.getSliceId();
                            redissonClient.getBucket(resultKey).delete();
                            
                            // 从工作节点任务列表中移除
                            String workerId = getSliceWorker(taskId, slice.getSliceId());
                            if (workerId != null) {
                                RSet<String> workerSlices = redissonClient.getSet(WORKER_SLICES_KEY + workerId);
                                workerSlices.remove(taskId + ":" + slice.getSliceId());
                            }
                            
                            // 删除分片工作节点映射
                            String workerKey = SLICE_WORKER_KEY + taskId + ":" + slice.getSliceId();
                            redissonClient.getBucket(workerKey).delete();
                        }
                        
                        // 删除任务状态
                        redissonClient.getBucket(TASK_STATUS_KEY + taskId).delete();
                        // 删除任务元数据
                        redissonClient.getMap(TASK_METADATA_KEY + taskId).delete();
                        // 删除任务分片
                        redissonClient.getMap(TASK_SLICES_KEY + taskId).delete();
                        // 删除任务结果
                        redissonClient.getBucket(TASK_RESULT_KEY + taskId).delete();
                        // 删除任务进度
                        redissonClient.getBucket(TASK_PROGRESS_KEY + taskId + ":progress").delete();
                        redissonClient.getBucket(TASK_PROGRESS_KEY + taskId + ":message").delete();
                        
                        // 从运行中任务集合中移除
                        RSet<String> runningTasks = redissonClient.getSet(RUNNING_TASKS_KEY);
                        runningTasks.remove(taskId);
                        
                        return true;
                    } finally {
                        lock.unlock();
                    }
                } else {
                    logger.warn("获取分布式锁超时: taskId={}", taskId);
                    return false;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("移除任务被中断: taskId={}", taskId, e);
                return false;
            }
        } catch (Exception e) {
            logger.error("移除任务失败: taskId={}", taskId, e);
            return false;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public List<String> getRunningTasks() {
        try {
            RSet<String> runningTasks = redissonClient.getSet(RUNNING_TASKS_KEY);
            if (runningTasks.isEmpty()) {
                return new ArrayList<>();
            }
            
            return new ArrayList<>(runningTasks.readAll());
        } catch (Exception e) {
            logger.error("获取运行中任务失败", e);
            return new ArrayList<>();
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> getWorkerHeartbeat(String workerId) {
        try {
            RMap<String, Object> hash = redissonClient.getMap(WORKER_HEARTBEAT_KEY + workerId);
            Map<String, Object> heartbeat = hash.readAllMap();
            return heartbeat.isEmpty() ? null : heartbeat;
        } catch (Exception e) {
            logger.error("获取工作节点心跳信息失败: workerId={}", workerId, e);
            return null;
        }
    }
    
    @Override
    public boolean updateWorkerHeartbeat(String workerId, Map<String, Object> heartbeatInfo) {
        if (heartbeatInfo == null || heartbeatInfo.isEmpty()) {
            return false;
        }
        
        try {
            RMap<String, Object> hash = redissonClient.getMap(WORKER_HEARTBEAT_KEY + workerId);
            hash.putAll(heartbeatInfo);
            hash.expire(HEARTBEAT_EXPIRE_SECONDS, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            logger.error("更新工作节点心跳信息失败: workerId={}", workerId, e);
            return false;
        }
    }
    
    @Override
    public boolean registerWorker(String workerId, Map<String, Object> initialInfo) {
        try {
            // 添加到全局Worker集合
            RSet<String> allWorkers = redissonClient.getSet("parallel:workers:all");
            allWorkers.add(workerId);
            
            // 更新心跳信息
            return updateWorkerHeartbeat(workerId, initialInfo);
        } catch (Exception e) {
            logger.error("注册Worker失败: workerId={}", workerId, e);
            return false;
        }
    }
    
    @Override
    public Set<String> getAllRegisteredWorkers() {
        try {
            RSet<String> allWorkers = redissonClient.getSet("parallel:workers:all");
            return new HashSet<>(allWorkers.readAll());
        } catch (Exception e) {
            logger.error("获取所有Worker失败", e);
            return new HashSet<>();
        }
    }
    
    @Override
    public boolean cleanupTaskSlices(String taskId) {
        logger.info("清理任务分片数据: taskId={}", taskId);
        
        try {
            // 使用分布式锁确保操作原子性
            RLock lock = redissonClient.getLock("lock:task:cleanup:" + taskId);
            try {
                if (lock.tryLock(10, 30, TimeUnit.SECONDS)) {
                    try {
                        // 检查任务状态，只清理已完成的任务
                        String status = getTaskStatus(taskId);
                        if (!"COMPLETED".equals(status) && !"FAILED".equals(status) && !"CANCELLED".equals(status)) {
                            logger.warn("任务未完成，不能清理数据: taskId={}, status={}", taskId, status);
                            return false;
                        }
                        
                        // 获取所有分片ID
                        List<TaskSlice<?>> slices = (List<TaskSlice<?>>) (Object) getTaskSlices(taskId);
                        if (slices != null && !slices.isEmpty()) {
                            // 清理分片结果数据
                            for (TaskSlice<?> slice : slices) {
                                String sliceId = slice.getSliceId();
                                
                                // 删除分片结果
                                String resultKey = SLICE_RESULT_KEY + taskId + ":" + sliceId;
                                redissonClient.getBucket(resultKey).delete();
                                
                                // 删除分片工作节点映射
                                String workerKey = SLICE_WORKER_KEY + taskId + ":" + sliceId;
                                redissonClient.getBucket(workerKey).delete();
                                
                                // 获取工作节点ID，并从工作节点的任务列表中移除
                                String workerId = getSliceWorker(taskId, sliceId);
                                if (workerId != null && !workerId.isEmpty()) {
                                    cleanupWorkerSlices(workerId, taskId + ":" + sliceId);
                                }
                            }
                            
                            // 删除所有分片信息
                            redissonClient.getMap(TASK_SLICES_KEY + taskId).delete();
                        }
                        
                        return true;
                    } finally {
                        lock.unlock();
                    }
                } else {
                    logger.warn("获取分布式锁超时: taskId={}", taskId);
                    return false;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("清理任务分片被中断: taskId={}", taskId, e);
                return false;
            }
        } catch (Exception e) {
            logger.error("清理任务分片失败: taskId={}, error={}", taskId, e.getMessage(), e);
            return false;
        }
    }
    
    @Override
    public boolean cleanupWorkerSlices(String workerId, String taskSliceKey) {
        try {
            RSet<String> workerSlices = redissonClient.getSet(WORKER_SLICES_KEY + workerId);
            return workerSlices.remove(taskSliceKey);
        } catch (Exception e) {
            logger.error("清理Worker分片数据失败: workerId={}, taskSliceKey={}", workerId, taskSliceKey, e);
            return false;
        }
    }
    
    @Override
    public boolean setTaskDataExpiry(String taskId, long expiryTimeInSeconds) {
        try {
            // 记录任务过期时间
            long expiryTimestamp = System.currentTimeMillis() + (expiryTimeInSeconds * 1000);
            RBucket<Long> expiryBucket = redissonClient.getBucket(TASK_EXPIRY_KEY + taskId);
            expiryBucket.set(expiryTimestamp, expiryTimeInSeconds, TimeUnit.SECONDS);
            
            return true;
        } catch (Exception e) {
            logger.error("设置任务数据过期时间失败: taskId={}, expiryTime={}", taskId, expiryTimeInSeconds, e);
            return false;
        }
    }
    
    @Override
    public int cleanupExpiredTasks() {
        logger.info("开始清理过期任务数据");
        
        int cleanedCount = 0;
        try {
            RSet<String> completedTasks = redissonClient.getSet(COMPLETED_TASKS_KEY);
            Set<String> tasks = new HashSet<>(completedTasks.readAll());
            
            long currentTime = System.currentTimeMillis();
            
            for (String taskId : tasks) {
                try {
                    // 检查任务是否过期
                    RBucket<Long> expiryBucket = redissonClient.getBucket(TASK_EXPIRY_KEY + taskId);
                    Long expiryTimestamp = expiryBucket.get();
                    
                    if (expiryTimestamp != null && expiryTimestamp < currentTime) {
                        // 任务已过期，可以清理
                        if (removeTask(taskId)) {
                            cleanedCount++;
                            logger.info("已清理过期任务: taskId={}", taskId);
                            
                            // 从已完成任务列表移除
                            completedTasks.remove(taskId);
                        }
                    }
                } catch (Exception e) {
                    logger.error("清理过期任务失败: taskId={}", taskId, e);
                }
            }
            
            logger.info("任务数据清理完成，共清理{}个过期任务", cleanedCount);
        } catch (Exception e) {
            logger.error("清理过期任务数据失败", e);
        }
        
        return cleanedCount;
    }
} 