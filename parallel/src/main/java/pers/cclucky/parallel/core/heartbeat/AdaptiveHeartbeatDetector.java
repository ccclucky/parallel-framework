package pers.cclucky.parallel.core.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.core.storage.TaskStorage;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 自适应心跳检测器实现
 * 实现动态调整的心跳机制，有效降低误判率
 */
public class AdaptiveHeartbeatDetector implements HeartbeatDetector {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveHeartbeatDetector.class);

    // 心跳检测初始间隔（毫秒）
    private static final long DEFAULT_HEARTBEAT_INTERVAL = 3000;
    // 心跳超时倍数（实际超时时间 = 当前心跳间隔 * 超时倍数）
    private static final double DEFAULT_TIMEOUT_FACTOR = 3.0;
    // 连续失败次数阈值，超过该值判定节点失效
    private static final int DEFAULT_FAILURE_THRESHOLD = 3;
    // 滑动窗口大小，用于计算心跳延迟的统计指标
    private static final int DEFAULT_WINDOW_SIZE = 10;
    // 自适应调整最小间隔（毫秒）
    private static final long MIN_HEARTBEAT_INTERVAL = 1000;
    // 自适应调整最大间隔（毫秒）
    private static final long MAX_HEARTBEAT_INTERVAL = 10000;
    
    // 任务存储
    private final TaskStorage taskStorage;
    
    // Worker状态信息
    private final Map<String, WorkerStatus> workerStatusMap = new ConcurrentHashMap<>();
    
    // 执行定时检测的线程池
    private ScheduledExecutorService scheduler;
    
    // 执行心跳检测任务的线程池
    private ThreadPoolExecutor executorService;
    
    // 检测是否运行
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    // 失效监听器
    private final List<WorkerFailureListener> failureListeners = new CopyOnWriteArrayList<>();
    
    /**
     * 创建自适应心跳检测器
     * @param taskStorage 任务存储
     */
    public AdaptiveHeartbeatDetector(TaskStorage taskStorage) {
        this.taskStorage = taskStorage;
    }
    
    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("启动自适应心跳检测服务");
            
            // 创建定时调度线程池
            scheduler = Executors.newScheduledThreadPool(1, r -> {
                Thread thread = new Thread(r, "heartbeat-scheduler");
                thread.setDaemon(true);
                return thread;
            });
            
            // 创建执行心跳检测的线程池
            executorService = new ThreadPoolExecutor(
                    4, 8, 60, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(1000),
                    r -> {
                        Thread thread = new Thread(r, "heartbeat-worker");
                        thread.setDaemon(true);
                        return thread;
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );
            
            // 启动定时心跳检测
            scheduler.scheduleWithFixedDelay(this::checkAllWorkers, 0, 1, TimeUnit.SECONDS);
            
            logger.info("自适应心跳检测服务已启动");
        } else {
            logger.warn("心跳检测服务已经在运行中");
        }
    }
    
    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("停止自适应心跳检测服务");
            
            // 关闭线程池
            if (scheduler != null) {
                scheduler.shutdown();
                try {
                    if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        scheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    scheduler.shutdownNow();
                }
                scheduler = null;
            }
            
            if (executorService != null) {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    executorService.shutdownNow();
                }
                executorService = null;
            }
            
            logger.info("自适应心跳检测服务已停止");
        } else {
            logger.warn("心跳检测服务已经停止");
        }
    }
    
    @Override
    public boolean registerWorker(String workerId, Map<String, Object> initialInfo) {
        if (workerId == null || workerId.isEmpty()) {
            return false;
        }
        
        // 初始化Worker状态
        WorkerStatus status = new WorkerStatus(workerId);
        workerStatusMap.put(workerId, status);
        
        // 保存到Redis（为所有节点可见）
        return taskStorage.registerWorker(workerId, initialInfo);
    }
    
    @Override
    public boolean unregisterWorker(String workerId) {
        if (workerId == null || workerId.isEmpty()) {
            return false;
        }
        
        // 移除Worker状态
        workerStatusMap.remove(workerId);
        return true;
    }
    
    @Override
    public boolean updateHeartbeat(String workerId, Map<String, Object> heartbeatInfo) {
        if (workerId == null || workerId.isEmpty()) {
            return false;
        }
        
        // 获取Worker状态
        WorkerStatus status = workerStatusMap.get(workerId);
        if (status == null) {
            // 如果不存在，则注册新Worker
            status = new WorkerStatus(workerId);
            workerStatusMap.put(workerId, status);
        }
        
        // 更新状态信息
        status.updateHeartbeat();
        status.resetFailureCount();
        
        // 存储心跳信息
        return taskStorage.updateWorkerHeartbeat(workerId, heartbeatInfo);
    }
    
    @Override
    public boolean isWorkerAlive(String workerId) {
        if (workerId == null || workerId.isEmpty()) {
            return false;
        }
        
        WorkerStatus status = workerStatusMap.get(workerId);
        if (status == null) {
            return false;
        }
        
        return !status.isDead();
    }
    
    @Override
    public Set<String> getAliveWorkers() {
        // 先从Redis获取所有注册的Worker
        Set<String> registeredWorkers = taskStorage.getAllRegisteredWorkers();
        Set<String> aliveWorkers = new HashSet<>();
        
        // 检查每个Worker的活跃状态
        for (String workerId : registeredWorkers) {
            // 如果本地没有状态信息，先添加
            if (!workerStatusMap.containsKey(workerId)) {
                WorkerStatus status = new WorkerStatus(workerId);
                workerStatusMap.put(workerId, status);
            }
            
            // 根据最新心跳判断是否活跃
            Map<String, Object> heartbeat = taskStorage.getWorkerHeartbeat(workerId);
            if (isHeartbeatValid(heartbeat)) {
                aliveWorkers.add(workerId);
            }
        }
        
        return aliveWorkers;
    }
    
    @Override
    public Set<String> getDeadWorkers() {
        Set<String> deadWorkers = new HashSet<>();
        
        for (Map.Entry<String, WorkerStatus> entry : workerStatusMap.entrySet()) {
            if (entry.getValue().isDead()) {
                deadWorkers.add(entry.getKey());
            }
        }
        
        return deadWorkers;
    }
    
    @Override
    public Map<String, Object> getWorkerHeartbeat(String workerId) {
        if (workerId == null || workerId.isEmpty()) {
            return Collections.emptyMap();
        }
        
        return taskStorage.getWorkerHeartbeat(workerId);
    }
    
    @Override
    public void addWorkerFailureListener(WorkerFailureListener listener) {
        if (listener != null && !failureListeners.contains(listener)) {
            failureListeners.add(listener);
        }
    }
    
    @Override
    public void removeWorkerFailureListener(WorkerFailureListener listener) {
        failureListeners.remove(listener);
    }
    
    /**
     * 检查所有Worker的状态
     */
    private void checkAllWorkers() {
        if (!running.get()) {
            return;
        }
        
        // 遍历所有Worker
        for (Map.Entry<String, WorkerStatus> entry : workerStatusMap.entrySet()) {
            String workerId = entry.getKey();
            WorkerStatus status = entry.getValue();
            
            // 提交异步检测任务
            executorService.execute(() -> checkWorker(workerId, status));
        }
    }
    
    /**
     * 检查单个Worker的状态
     * @param workerId Worker ID
     * @param status Worker状态
     */
    private void checkWorker(String workerId, WorkerStatus status) {
        try {
            // 检查是否需要执行心跳检测
            if (!status.shouldCheck()) {
                return;
            }
            
            // 获取最后心跳信息
            Map<String, Object> heartbeat = taskStorage.getWorkerHeartbeat(workerId);
            
            // 检查心跳是否有效
            if (isHeartbeatValid(heartbeat)) {
                // 有效心跳，重置失败计数
                status.resetFailureCount();
                
                // 根据当前网络延迟情况，调整心跳间隔
                adjustHeartbeatInterval(status, heartbeat);
            } else {
                // 无效心跳，增加失败计数
                status.incrementFailureCount();
                
                // 检查是否达到失效阈值
                if (status.getFailureCount() >= DEFAULT_FAILURE_THRESHOLD) {
                    // 标记为失效
                    if (!status.isDead()) {
                        status.markAsDead();
                        
                        // 通知监听器
                        notifyWorkerFailure(workerId, heartbeat);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("检查Worker状态失败: workerId={}", workerId, e);
        }
    }
    
    /**
     * 检查心跳信息是否有效
     * @param heartbeat 心跳信息
     * @return 是否有效
     */
    private boolean isHeartbeatValid(Map<String, Object> heartbeat) {
        if (heartbeat == null || heartbeat.isEmpty()) {
            return false;
        }
        
        // 检查更新时间
        Object updateTimeObj = heartbeat.get("heartbeatTime");
        if (updateTimeObj == null) {
            return false;
        }
        
        long updateTime;
        if (updateTimeObj instanceof Number) {
            updateTime = ((Number) updateTimeObj).longValue();
        } else if (updateTimeObj instanceof String) {
            try {
                updateTime = Long.parseLong((String) updateTimeObj);
            } catch (NumberFormatException e) {
                return false;
            }
        } else {
            return false;
        }
        
        // 计算时间差
        long now = System.currentTimeMillis();
        long diff = now - updateTime;
        
        // 检查是否超过默认超时时间
        return diff <= DEFAULT_HEARTBEAT_INTERVAL * DEFAULT_TIMEOUT_FACTOR;
    }
    
    /**
     * 调整心跳间隔
     * @param status Worker状态
     * @param heartbeat 心跳信息
     */
    private void adjustHeartbeatInterval(WorkerStatus status, Map<String, Object> heartbeat) {
        // 计算响应延迟
        long now = System.currentTimeMillis();
        Object updateTimeObj = heartbeat.get("updateTime");
        if (updateTimeObj == null) {
            return;
        }
        
        long updateTime;
        if (updateTimeObj instanceof Number) {
            updateTime = ((Number) updateTimeObj).longValue();
        } else {
            try {
                updateTime = Long.parseLong(updateTimeObj.toString());
            } catch (NumberFormatException e) {
                return;
            }
        }
        
        // 计算延迟
        long delay = now - updateTime;
        
        // 更新延迟窗口
        status.addDelayToWindow(delay);
        
        // 计算新的心跳间隔
        long newInterval = calculateNewHeartbeatInterval(status);
        status.setHeartbeatInterval(newInterval);
    }
    
    /**
     * 计算新的心跳间隔
     * @param status Worker状态
     * @return 新的心跳间隔
     */
    private long calculateNewHeartbeatInterval(WorkerStatus status) {
        // 获取延迟统计
        double avgDelay = status.getAverageDelay();
        double stdDev = status.getStandardDeviation();
        
        // 计算基于统计的自适应间隔
        long interval = (long) (avgDelay + 2 * stdDev);
        
        // 确保在最小和最大间隔之间
        interval = Math.max(MIN_HEARTBEAT_INTERVAL, interval);
        interval = Math.min(MAX_HEARTBEAT_INTERVAL, interval);
        
        return interval;
    }
    
    /**
     * 通知Worker失效
     * @param workerId Worker ID
     * @param lastHeartbeat 最后心跳信息
     */
    private void notifyWorkerFailure(String workerId, Map<String, Object> lastHeartbeat) {
        logger.warn("Worker节点失效: workerId={}", workerId);
        
        // 通知所有监听器
        for (WorkerFailureListener listener : failureListeners) {
            try {
                listener.onWorkerFailure(workerId, lastHeartbeat);
            } catch (Exception e) {
                logger.error("通知Worker失效事件失败: workerId={}", workerId, e);
            }
        }
    }
    
    /**
     * Worker状态类
     */
    private static class WorkerStatus {
        // Worker ID
        private final String workerId;
        // 失效标记
        private final AtomicBoolean dead = new AtomicBoolean(false);
        // 连续失败次数
        private int failureCount;
        // 最后心跳时间
        private long lastHeartbeatTime;
        // 心跳间隔
        private long heartbeatInterval;
        // 最后检测时间
        private long lastCheckTime;
        // 延迟窗口
        private final LinkedList<Long> delayWindow;
        
        /**
         * 创建Worker状态
         * @param workerId Worker ID
         */
        public WorkerStatus(String workerId) {
            this.workerId = workerId;
            this.failureCount = 0;
            this.lastHeartbeatTime = System.currentTimeMillis();
            this.heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
            this.lastCheckTime = 0;
            this.delayWindow = new LinkedList<>();
        }
        
        /**
         * 检查是否应该执行检测
         * @return 是否应该检测
         */
        public boolean shouldCheck() {
            long now = System.currentTimeMillis();
            return now - lastCheckTime >= heartbeatInterval;
        }
        
        /**
         * 更新心跳时间
         */
        public void updateHeartbeat() {
            this.lastHeartbeatTime = System.currentTimeMillis();
            this.lastCheckTime = System.currentTimeMillis();
        }
        
        /**
         * 增加失败次数
         */
        public void incrementFailureCount() {
            this.failureCount++;
            this.lastCheckTime = System.currentTimeMillis();
        }
        
        /**
         * 重置失败次数
         */
        public void resetFailureCount() {
            this.failureCount = 0;
        }
        
        /**
         * 获取失败次数
         * @return 失败次数
         */
        public int getFailureCount() {
            return failureCount;
        }
        
        /**
         * 标记为失效
         */
        public void markAsDead() {
            this.dead.set(true);
        }
        
        /**
         * 标记为有效
         */
        public void markAsAlive() {
            this.dead.set(false);
        }
        
        /**
         * 检查是否失效
         * @return 是否失效
         */
        public boolean isDead() {
            return dead.get();
        }
        
        /**
         * 设置心跳间隔
         * @param interval 心跳间隔
         */
        public void setHeartbeatInterval(long interval) {
            this.heartbeatInterval = interval;
        }
        
        /**
         * 添加延迟到窗口
         * @param delay 延迟
         */
        public void addDelayToWindow(long delay) {
            // 加入窗口
            delayWindow.addLast(delay);
            
            // 保持窗口大小
            while (delayWindow.size() > DEFAULT_WINDOW_SIZE) {
                delayWindow.removeFirst();
            }
        }
        
        /**
         * 获取平均延迟
         * @return 平均延迟
         */
        public double getAverageDelay() {
            if (delayWindow.isEmpty()) {
                return DEFAULT_HEARTBEAT_INTERVAL;
            }
            
            double sum = 0;
            for (long delay : delayWindow) {
                sum += delay;
            }
            
            return sum / delayWindow.size();
        }
        
        /**
         * 获取标准差
         * @return 标准差
         */
        public double getStandardDeviation() {
            if (delayWindow.size() < 2) {
                return DEFAULT_HEARTBEAT_INTERVAL / 2.0;
            }
            
            double avg = getAverageDelay();
            double sum = 0;
            
            for (long delay : delayWindow) {
                double diff = delay - avg;
                sum += diff * diff;
            }
            
            return Math.sqrt(sum / (delayWindow.size() - 1));
        }
    }
} 