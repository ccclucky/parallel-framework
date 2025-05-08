package pers.cclucky.parallel.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pers.cclucky.parallel.ParallelFramework;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * 性能测试客户端
 */
public class PerformanceTestClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(PerformanceTestClient.class);
    
    // 测试配置
    private final String testDataDir;
    private final int concurrentTasks;
    private final long taskTimeoutMs;
    private final int priority;
    private final boolean retryable;
    private final int maxRetryCount;
    private final List<ParallelFramework> frameworks;
    
    // 测试统计
    private final AtomicInteger submittedTasks = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger failedTasks = new AtomicInteger(0);
    private final AtomicInteger cancelledTasks = new AtomicInteger(0);
    private final AtomicLong totalProcessingTime = new AtomicLong(0);
    private final List<Long> processingTimes = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrentHashMap<String, Long> taskStartTimes = new ConcurrentHashMap<>();
    
    // 控制并发的信号量
    private final Semaphore taskSemaphore;
    private final AtomicBoolean testStopped = new AtomicBoolean(false);
    
    // 执行器
    private ExecutorService executorService;
    
    /**
     * 自定义简化的TaskResult实现
     */
    public static class TestTaskResult {
        private final boolean success;
        private final String result;
        private final long processTime;
        
        public TestTaskResult(boolean success, String result, long processTime) {
            this.success = success;
            this.result = result;
            this.processTime = processTime;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public String getResult() {
            return result;
        }
        
        public long getProcessTime() {
            return processTime;
        }
    }
    
    /**
     * 构造性能测试客户端
     *
     * @param testDataDir 测试数据目录
     * @param concurrentTasks 并发任务数
     * @param taskTimeoutMs 任务超时时间(毫秒)
     * @param priority 任务优先级
     * @param retryable 是否可重试
     * @param maxRetryCount 最大重试次数
     * @param frameworks 框架实例列表
     */
    public PerformanceTestClient(String testDataDir, int concurrentTasks, long taskTimeoutMs,
                                int priority, boolean retryable, int maxRetryCount,
                                List<ParallelFramework> frameworks) {
        this.testDataDir = testDataDir;
        this.concurrentTasks = concurrentTasks;
        this.taskTimeoutMs = taskTimeoutMs;
        this.priority = priority;
        this.retryable = retryable;
        this.maxRetryCount = maxRetryCount;
        this.frameworks = frameworks;
        this.taskSemaphore = new Semaphore(concurrentTasks);
    }
    
    /**
     * 运行性能测试
     *
     * @param maxTaskCount 最大任务数量，如果小于等于0则处理所有测试数据
     * @param timeoutMinutes 测试运行超时时间(分钟)
     * @return 测试结果
     * @throws Exception 如果测试执行过程中发生错误
     */
    public TestResult runTest(int maxTaskCount, int timeoutMinutes) throws Exception {
        LOGGER.info("开始性能测试，并发任务: {}, 超时: {}分钟", concurrentTasks, timeoutMinutes);
        
        // 重置统计数据
        submittedTasks.set(0);
        completedTasks.set(0);
        failedTasks.set(0);
        cancelledTasks.set(0);
        totalProcessingTime.set(0);
        processingTimes.clear();
        taskStartTimes.clear();
        testStopped.set(false);
        
        // 准备测试数据文件
        File dataDir = new File(testDataDir);
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            throw new IllegalArgumentException("测试数据目录不存在: " + testDataDir);
        }
        
        File[] dataFiles = dataDir.listFiles((dir, name) -> 
                name.startsWith("data_file_") && (name.endsWith(".json") || name.endsWith(".gz")));
                
        if (dataFiles == null || dataFiles.length == 0) {
            throw new IllegalArgumentException("测试数据目录中没有找到数据文件: " + testDataDir);
        }
        
        LOGGER.info("发现{}个测试数据文件", dataFiles.length);
        
        // 准备执行器
        executorService = Executors.newFixedThreadPool(concurrentTasks);
        
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        
        // 创建任务提交和监控线程
        CountDownLatch testCompleteLatch = new CountDownLatch(1);
        
        // 启动任务提交线程
        Thread submitterThread = new Thread(() -> {
            try {
                int totalTaskCount = 0;
                
                // 处理每个数据文件
                for (File dataFile : dataFiles) {
                    boolean isGzip = dataFile.getName().endsWith(".gz");
                    
                    try (BufferedReader reader = new BufferedReader(
                            isGzip ? 
                            new InputStreamReader(new GZIPInputStream(new FileInputStream(dataFile)), StandardCharsets.UTF_8) :
                            new InputStreamReader(new FileInputStream(dataFile), StandardCharsets.UTF_8))) {
                        
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (maxTaskCount > 0 && totalTaskCount >= maxTaskCount) {
                                break;
                            }
                            
                            if (!line.trim().isEmpty()) {
                                // 等待信号量许可
                                if (!taskSemaphore.tryAcquire(5, TimeUnit.SECONDS)) {
                                    if (testStopped.get()) {
                                        break;
                                    }
                                    continue;
                                }
                                
                                if (testStopped.get()) {
                                    taskSemaphore.release();
                                    break;
                                }
                                
                                // 提交任务
                                totalTaskCount++;
                                submitTask(line);
                            }
                            
                            // 检查是否超时
                            if (System.currentTimeMillis() - startTime > timeoutMinutes * 60 * 1000) {
                                LOGGER.info("测试超时，停止提交任务");
                                testStopped.set(true);
                                break;
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.error("读取测试数据文件错误: {} - {}", dataFile.getName(), e.getMessage());
                    }
                    
                    if (maxTaskCount > 0 && totalTaskCount >= maxTaskCount || testStopped.get()) {
                        break;
                    }
                }
                
                LOGGER.info("所有任务已提交，共{}个", submittedTasks.get());
                
                // 等待所有任务完成
                while (completedTasks.get() + failedTasks.get() + cancelledTasks.get() < submittedTasks.get()) {
                    Thread.sleep(100);
                    
                    // 检查是否超时
                    if (System.currentTimeMillis() - startTime > timeoutMinutes * 60 * 1000) {
                        LOGGER.warn("等待任务完成超时，取消剩余任务");
                        testStopped.set(true);
                        break;
                    }
                }
                
            } catch (Exception e) {
                LOGGER.error("任务提交线程发生错误", e);
            } finally {
                testCompleteLatch.countDown();
            }
        });
        
        submitterThread.start();
        
        // 等待测试完成或超时
        boolean completed = testCompleteLatch.await(timeoutMinutes, TimeUnit.MINUTES);
        if (!completed) {
            LOGGER.warn("测试执行超时，强制终止");
            testStopped.set(true);
        }
        
        // 关闭执行器
        executorService.shutdown();
        if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
            executorService.shutdownNow();
        }
        
        // 计算测试时间
        long endTime = System.currentTimeMillis();
        long totalTestTimeMs = endTime - startTime;
        
        // 生成测试结果
        TestResult result = new TestResult();
        result.setStartTime(startTime);
        result.setEndTime(endTime);
        result.setTotalTestTimeMs(totalTestTimeMs);
        result.setSubmittedTasks(submittedTasks.get());
        result.setCompletedTasks(completedTasks.get());
        result.setFailedTasks(failedTasks.get());
        result.setCancelledTasks(cancelledTasks.get());
        
        if (!processingTimes.isEmpty()) {
            // 计算处理时间统计
            long sumProcessingTime = totalProcessingTime.get();
            long avgProcessingTime = sumProcessingTime / processingTimes.size();
            
            // 计算中位数和百分位
            List<Long> sortedTimes = new ArrayList<>(processingTimes);
            Collections.sort(sortedTimes);
            
            long medianProcessingTime = sortedTimes.get(sortedTimes.size() / 2);
            long p95ProcessingTime = sortedTimes.get((int)(sortedTimes.size() * 0.95));
            long p99ProcessingTime = sortedTimes.get((int)(sortedTimes.size() * 0.99));
            long minProcessingTime = sortedTimes.get(0);
            long maxProcessingTime = sortedTimes.get(sortedTimes.size() - 1);
            
            result.setMinProcessingTimeMs(minProcessingTime);
            result.setMaxProcessingTimeMs(maxProcessingTime);
            result.setAvgProcessingTimeMs(avgProcessingTime);
            result.setMedianProcessingTimeMs(medianProcessingTime);
            result.setP95ProcessingTimeMs(p95ProcessingTime);
            result.setP99ProcessingTimeMs(p99ProcessingTime);
        }
        
        LOGGER.info("性能测试完成，耗时: {}秒", totalTestTimeMs / 1000.0);
        
        return result;
    }
    
    /**
     * 提交任务
     *
     * @param data 任务数据
     */
    private void submitTask(String data) {
        executorService.submit(() -> {
            try {
                ParallelFramework framework = selectFramework();
                String taskId = UUID.randomUUID().toString();
                
                // 记录任务开始时间
                long taskStartTime = System.currentTimeMillis();
                taskStartTimes.put(taskId, taskStartTime);
                
                try {
                    // 提交任务 - 这是一个模拟实现，实际中需要根据ParallelFramework的API调整
                    submittedTasks.incrementAndGet();
                    
                    // 处理数据 - 模拟处理过程
                    String resultStr = processData(data);
                    
                    // 模拟处理结果
                    TestTaskResult result = new TestTaskResult(true, resultStr, 0);
                    
                    // 记录任务完成时间和结果
                    long taskEndTime = System.currentTimeMillis();
                    long processingTime = taskEndTime - taskStartTime;
                    
                    completedTasks.incrementAndGet();
                    totalProcessingTime.addAndGet(processingTime);
                    processingTimes.add(processingTime);
                    
                } catch (Exception e) {
                    LOGGER.error("任务执行异常: {}", e.getMessage());
                    failedTasks.incrementAndGet();
                }
                
                // 定期记录进度
                int submitted = submittedTasks.get();
                if (submitted % 100 == 0) {
                    LOGGER.info("进度: {}/{} 完成, {} 失败, {} 取消", 
                            completedTasks.get(), submitted, failedTasks.get(), cancelledTasks.get());
                }
                
            } finally {
                // 释放信号量
                taskSemaphore.release();
            }
        });
    }
    
    /**
     * 处理数据 - 模拟处理过程
     * 与SimpleTaskProcessor使用相同的逻辑以便比较
     * 
     * @param data 输入数据
     * @return 处理结果
     */
    private String processData(String data) {
        // 高效的字符串反转
        StringBuilder sb = new StringBuilder(data);
        return sb.reverse().toString();
    }
    
    /**
     * 选择框架实例用于提交任务
     * 使用简单的轮询策略
     *
     * @return 选择的框架实例
     */
    private ParallelFramework selectFramework() {
        int index = (int)(submittedTasks.get() % frameworks.size());
        return frameworks.get(index);
    }
    
    /**
     * 性能测试结果
     */
    public static class TestResult {
        private long startTime;
        private long endTime;
        private long totalTestTimeMs;
        private int submittedTasks;
        private int completedTasks;
        private int failedTasks;
        private int cancelledTasks;
        private long minProcessingTimeMs;
        private long maxProcessingTimeMs;
        private long avgProcessingTimeMs;
        private long medianProcessingTimeMs;
        private long p95ProcessingTimeMs;
        private long p99ProcessingTimeMs;
        
        public long getStartTime() {
            return startTime;
        }
        
        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }
        
        public long getEndTime() {
            return endTime;
        }
        
        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }
        
        public long getTotalTestTimeMs() {
            return totalTestTimeMs;
        }
        
        public void setTotalTestTimeMs(long totalTestTimeMs) {
            this.totalTestTimeMs = totalTestTimeMs;
        }
        
        public int getSubmittedTasks() {
            return submittedTasks;
        }
        
        public void setSubmittedTasks(int submittedTasks) {
            this.submittedTasks = submittedTasks;
        }
        
        public int getCompletedTasks() {
            return completedTasks;
        }
        
        public void setCompletedTasks(int completedTasks) {
            this.completedTasks = completedTasks;
        }
        
        public int getFailedTasks() {
            return failedTasks;
        }
        
        public void setFailedTasks(int failedTasks) {
            this.failedTasks = failedTasks;
        }
        
        public int getCancelledTasks() {
            return cancelledTasks;
        }
        
        public void setCancelledTasks(int cancelledTasks) {
            this.cancelledTasks = cancelledTasks;
        }
        
        public long getMinProcessingTimeMs() {
            return minProcessingTimeMs;
        }
        
        public void setMinProcessingTimeMs(long minProcessingTimeMs) {
            this.minProcessingTimeMs = minProcessingTimeMs;
        }
        
        public long getMaxProcessingTimeMs() {
            return maxProcessingTimeMs;
        }
        
        public void setMaxProcessingTimeMs(long maxProcessingTimeMs) {
            this.maxProcessingTimeMs = maxProcessingTimeMs;
        }
        
        public long getAvgProcessingTimeMs() {
            return avgProcessingTimeMs;
        }
        
        public void setAvgProcessingTimeMs(long avgProcessingTimeMs) {
            this.avgProcessingTimeMs = avgProcessingTimeMs;
        }
        
        public long getMedianProcessingTimeMs() {
            return medianProcessingTimeMs;
        }
        
        public void setMedianProcessingTimeMs(long medianProcessingTimeMs) {
            this.medianProcessingTimeMs = medianProcessingTimeMs;
        }
        
        public long getP95ProcessingTimeMs() {
            return p95ProcessingTimeMs;
        }
        
        public void setP95ProcessingTimeMs(long p95ProcessingTimeMs) {
            this.p95ProcessingTimeMs = p95ProcessingTimeMs;
        }
        
        public long getP99ProcessingTimeMs() {
            return p99ProcessingTimeMs;
        }
        
        public void setP99ProcessingTimeMs(long p99ProcessingTimeMs) {
            this.p99ProcessingTimeMs = p99ProcessingTimeMs;
        }
        
        /**
         * 计算吞吐量（每秒任务数）
         * @return 每秒处理的任务数
         */
        public double getThroughputPerSecond() {
            if (totalTestTimeMs > 0) {
                return completedTasks * 1000.0 / totalTestTimeMs;
            }
            return 0;
        }
        
        /**
         * 计算任务成功率
         * @return 成功率（0-1）
         */
        public double getSuccessRate() {
            if (submittedTasks > 0) {
                return (double) completedTasks / submittedTasks;
            }
            return 0;
        }
    }
} 