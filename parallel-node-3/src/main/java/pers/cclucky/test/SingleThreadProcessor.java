package pers.cclucky.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单线程处理器
 * 用于与并行框架进行性能对比
 */
public class SingleThreadProcessor {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleThreadProcessor.class);
    
    // 测试结果统计
    private final List<Long> processingTimes = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger failedTasks = new AtomicInteger(0);
    
    /**
     * 执行单线程测试
     * 
     * @param testStrings 测试字符串列表
     * @param config 测试配置
     * @return 测试结果
     */
    public ParallelPerformanceTest.TestResult runTest(List<String> testStrings, ParallelPerformanceTest.TestConfig config) {
        logger.info("开始单线程性能测试，处理 {} 个字符串", testStrings.size());
        
        // 重置统计数据
        processingTimes.clear();
        completedTasks.set(0);
        failedTasks.set(0);
        
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        
        // 逐个处理所有字符串
        for (String testString : testStrings) {
            try {
                long taskStartTime = System.currentTimeMillis();
                
                // 直接在单线程中反转字符串
                String result = new StringBuilder(testString).reverse().toString();

                Thread.sleep(testString.length());
                
                long taskEndTime = System.currentTimeMillis();
                long processingTime = taskEndTime - taskStartTime;
                
                // 记录处理时间
                processingTimes.add(processingTime);

                // 增加完成计数
                completedTasks.incrementAndGet();
                
                // 定期记录进度
                int completed = completedTasks.get();
                if (completed % 100 == 0) {
                    logger.info("单线程进度: {}/{} 完成", 
                            completed, testStrings.size());
                }
            } catch (Exception e) {
                logger.error("单线程处理异常", e);
                failedTasks.incrementAndGet();
            }
        }
        
        // 计算测试时间
        long endTime = System.currentTimeMillis();
        long totalTestTimeMs = endTime - startTime;
        
        // 创建测试结果
        ParallelPerformanceTest.TestResult result = new ParallelPerformanceTest.TestResult();
        result.setStartTime(startTime);
        result.setEndTime(endTime);
        result.setTotalTestTimeMs(totalTestTimeMs);
        result.setTotalTasks(testStrings.size());
        result.setCompletedTasks(completedTasks.get());
        result.setFailedTasks(failedTasks.get());
        
        // 处理时间分析
        if (!processingTimes.isEmpty()) {
            // 排序处理时间用于计算中位数和百分位数
            List<Long> sortedTimes = new ArrayList<>(processingTimes);
            Collections.sort(sortedTimes);
            
            // 计算统计数据
            long totalTime = 0;
            for (Long time : processingTimes) {
                totalTime += time;
            }
            
            result.setMinProcessingTimeMs(sortedTimes.get(0));
            result.setMaxProcessingTimeMs(sortedTimes.get(sortedTimes.size() - 1));
            result.setAvgProcessingTimeMs(totalTime / processingTimes.size());
            result.setMedianProcessingTimeMs(sortedTimes.get(sortedTimes.size() / 2));
            
            // 计算95%分位数
            int p95Index = (int)(sortedTimes.size() * 0.95);
            result.setP95ProcessingTimeMs(sortedTimes.get(p95Index));
        }
        
        return result;
    }
} 