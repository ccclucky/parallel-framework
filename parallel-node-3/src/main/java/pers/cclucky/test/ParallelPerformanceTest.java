package pers.cclucky.test;

import pers.cclucky.StringReverseTask;
import pers.cclucky.parallel.ParallelFramework;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.client.ParallelClient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 并行框架性能测试器
 * 测试并行框架处理大量字符串反转任务的性能
 */
public class ParallelPerformanceTest {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParallelPerformanceTest.class);
    
    // 测试结果统计
    private final List<Long> processingTimes = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger failedTasks = new AtomicInteger(0);
    
    // 框架客户端
    private final ParallelClient client;
    
    // 测试配置
    private final TestConfig config;
    
    /**
     * 创建性能测试器
     * 
     * @param client 并行框架客户端
     * @param config 测试配置
     */
    public ParallelPerformanceTest(ParallelClient client, TestConfig config) {
        this.client = client;
        this.config = config;
    }
    
    /**
     * 执行性能测试
     * 
     * @return 测试结果
     * @throws Exception 如果测试执行过程中发生错误
     */
    public TestResult runTest() throws Exception {
        // 准备测试数据
        List<String> testStrings;
        if (config.isGenerateData()) {
            // 生成测试数据
            File dataDir = new File(config.getDataDir());
            if (!dataDir.exists()) {
                dataDir.mkdirs();
            }
            
            logger.info("生成测试数据...");
            StringDataGenerator.generateStringDataFiles(
                    config.getDataDir(),
                    config.getFileCount(),
                    config.getStringsPerFile(),
                    config.getMinStringLength(),
                    config.getMaxStringLength()
            );
            
            // 加载生成的测试数据
            testStrings = StringDataGenerator.loadTestStrings(config.getDataDir(), config.getMaxTasks());
        } else {
            // 直接加载已有的测试数据
            testStrings = StringDataGenerator.loadTestStrings(config.getDataDir(), config.getMaxTasks());
        }
        
        logger.info("加载了 {} 个测试字符串", testStrings.size());
        
        // 重置统计数据
        processingTimes.clear();
        completedTasks.set(0);
        failedTasks.set(0);
        
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(config.getConcurrentTasks());
        CountDownLatch latch = new CountDownLatch(testStrings.size());
        
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        
        // 提交所有任务
        for (String testString : testStrings) {
            executor.submit(() -> {
                try {
                    long taskStartTime = System.currentTimeMillis();
                    
                    // 创建任务
                    StringReverseTask task = new StringReverseTask(testString);
                    
                    // 提交任务并等待结果
                    TaskResult<String> result = client.submitAndWait(task);
                    
                    long taskEndTime = System.currentTimeMillis();
                    long processingTime = taskEndTime - taskStartTime;
                    
                    // 记录处理时间
                    processingTimes.add(processingTime);
                    
                    // 检查结果
                    if (result != null && result.isSuccess()) {
                        // 验证结果（如果需要）
                        String expected = new StringBuilder(testString).reverse().toString();
                        if (config.isVerifyResults() && !expected.equals(result.getResult())) {
                            logger.warn("结果验证失败: 预期 [{}], 实际 [{}]", expected, result.getResult());
                            failedTasks.incrementAndGet();
                        } else {
                            completedTasks.incrementAndGet();
                        }
                    } else {
                        failedTasks.incrementAndGet();
                    }
                    
                    // 定期记录进度
                    int completed = completedTasks.get();
                    if (completed % 100 == 0) {
                        logger.info("进度: {}/{} 完成, {} 失败", 
                                completed, testStrings.size(), failedTasks.get());
                    }
                } catch (Exception e) {
                    logger.error("任务执行异常", e);
                    failedTasks.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // 等待所有任务完成或超时
        boolean completed = latch.await(config.getTimeoutMinutes(), TimeUnit.MINUTES);
        if (!completed) {
            logger.warn("测试超时，部分任务未完成");
        }
        
        // 关闭线程池
        executor.shutdown();
        if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
            executor.shutdownNow();
        }
        
        // 计算测试时间
        long endTime = System.currentTimeMillis();
        long totalTestTimeMs = endTime - startTime;
        
        // 创建测试结果
        TestResult result = new TestResult();
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
        
        // 生成报告
        if (config.isGenerateReport()) {
            generateReport(result);
        }
        
        return result;
    }
    
    /**
     * 生成HTML报告
     * 
     * @param result 测试结果
     * @throws IOException 如果文件操作失败
     */
    private void generateReport(TestResult result) throws IOException {
        File reportDir = new File(config.getReportDir());
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        String reportPath = new File(reportDir, "performance_report_" + timestamp + ".html").getPath();
        
        logger.info("生成性能测试报告: {}", reportPath);
        
        try (FileWriter writer = new FileWriter(reportPath)) {
            writer.write("<!DOCTYPE html>\n");
            writer.write("<html lang=\"zh-CN\">\n");
            writer.write("<head>\n");
            writer.write("    <meta charset=\"UTF-8\">\n");
            writer.write("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
            writer.write("    <title>并行框架字符串反转性能测试报告</title>\n");
            writer.write("    <style>\n");
            writer.write("        body { font-family: 'Microsoft YaHei', Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; color: #333; }\n");
            writer.write("        .container { max-width: 1200px; margin: 0 auto; background-color: #fff; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }\n");
            writer.write("        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }\n");
            writer.write("        h2 { color: #2980b9; margin-top: 20px; }\n");
            writer.write("        table { width: 100%; border-collapse: collapse; margin: 20px 0; }\n");
            writer.write("        th, td { padding: 12px 15px; border: 1px solid #ddd; text-align: left; }\n");
            writer.write("        th { background-color: #3498db; color: white; }\n");
            writer.write("        tr:nth-child(even) { background-color: #f2f2f2; }\n");
            writer.write("    </style>\n");
            writer.write("</head>\n");
            writer.write("<body>\n");
            writer.write("    <div class=\"container\">\n");
            writer.write("        <h1>并行框架字符串反转性能测试报告</h1>\n");
            
            // 测试概述
            writer.write("        <h2>测试概述</h2>\n");
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>值</th></tr>\n");
            writer.write("            <tr><td>测试开始时间</td><td>" + new Date(result.getStartTime()) + "</td></tr>\n");
            writer.write("            <tr><td>测试结束时间</td><td>" + new Date(result.getEndTime()) + "</td></tr>\n");
            writer.write("            <tr><td>测试总耗时</td><td>" + formatTime(result.getTotalTestTimeMs()) + "</td></tr>\n");
            writer.write("            <tr><td>并发任务数</td><td>" + config.getConcurrentTasks() + "</td></tr>\n");
            writer.write("            <tr><td>测试数据目录</td><td>" + config.getDataDir() + "</td></tr>\n");
            writer.write("        </table>\n");
            
            // 任务统计
            writer.write("        <h2>任务统计</h2>\n");
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>数量</th><th>百分比</th></tr>\n");
            writer.write("            <tr><td>总任务数</td><td>" + result.getTotalTasks() + "</td><td>100%</td></tr>\n");
            writer.write("            <tr><td>成功任务数</td><td>" + result.getCompletedTasks() + "</td><td>" 
                    + String.format("%.2f%%", (result.getCompletedTasks() * 100.0 / result.getTotalTasks())) + "</td></tr>\n");
            writer.write("            <tr><td>失败任务数</td><td>" + result.getFailedTasks() + "</td><td>" 
                    + String.format("%.2f%%", (result.getFailedTasks() * 100.0 / result.getTotalTasks())) + "</td></tr>\n");
            writer.write("        </table>\n");
            
            // 性能指标
            writer.write("        <h2>性能指标</h2>\n");
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>值</th></tr>\n");
            writer.write("            <tr><td>吞吐量</td><td>" + String.format("%.2f 任务/秒", result.getThroughputPerSecond()) + "</td></tr>\n");
            
            if (result.getCompletedTasks() > 0) {
                writer.write("            <tr><td>最小处理时间</td><td>" + formatTime(result.getMinProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>最大处理时间</td><td>" + formatTime(result.getMaxProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>平均处理时间</td><td>" + formatTime(result.getAvgProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>中位数处理时间</td><td>" + formatTime(result.getMedianProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>95%分位处理时间</td><td>" + formatTime(result.getP95ProcessingTimeMs()) + "</td></tr>\n");
            }
            
            writer.write("        </table>\n");
            
            // 报告页脚
            writer.write("        <div style=\"margin-top: 50px; text-align: center; color: #7f8c8d; font-size: 12px;\">\n");
            writer.write("            <p>报告生成时间: " + new Date() + "</p>\n");
            writer.write("            <p>并行计算框架性能测试工具</p>\n");
            writer.write("        </div>\n");
            
            writer.write("    </div>\n");
            writer.write("</body>\n");
            writer.write("</html>\n");
        }
    }
    
    /**
     * 格式化时间
     */
    private static String formatTime(long timeMs) {
        if (timeMs < 1000) {
            return timeMs + " 毫秒";
        } else if (timeMs < 60000) {
            return String.format("%.2f 秒", timeMs / 1000.0);
        } else if (timeMs < 3600000) {
            return String.format("%.2f 分钟", timeMs / 60000.0);
        } else {
            return String.format("%.2f 小时", timeMs / 3600000.0);
        }
    }
    
    /**
     * 测试配置类
     */
    public static class TestConfig {
        private String dataDir = "test-data";
        private String reportDir = "test-reports";
        private int concurrentTasks = 10;
        private int maxTasks = 1000;
        private int timeoutMinutes = 10;
        private boolean generateData = true;
        private int fileCount = 5;
        private int stringsPerFile = 200;
        private int minStringLength = 10;
        private int maxStringLength = 1000;
        private boolean verifyResults = true;
        private boolean generateReport = true;
        
        public String getDataDir() {
            return dataDir;
        }
        
        public void setDataDir(String dataDir) {
            this.dataDir = dataDir;
        }
        
        public String getReportDir() {
            return reportDir;
        }
        
        public void setReportDir(String reportDir) {
            this.reportDir = reportDir;
        }
        
        public int getConcurrentTasks() {
            return concurrentTasks;
        }
        
        public void setConcurrentTasks(int concurrentTasks) {
            this.concurrentTasks = concurrentTasks;
        }
        
        public int getMaxTasks() {
            return maxTasks;
        }
        
        public void setMaxTasks(int maxTasks) {
            this.maxTasks = maxTasks;
        }
        
        public int getTimeoutMinutes() {
            return timeoutMinutes;
        }
        
        public void setTimeoutMinutes(int timeoutMinutes) {
            this.timeoutMinutes = timeoutMinutes;
        }
        
        public boolean isGenerateData() {
            return generateData;
        }
        
        public void setGenerateData(boolean generateData) {
            this.generateData = generateData;
        }
        
        public int getFileCount() {
            return fileCount;
        }
        
        public void setFileCount(int fileCount) {
            this.fileCount = fileCount;
        }
        
        public int getStringsPerFile() {
            return stringsPerFile;
        }
        
        public void setStringsPerFile(int stringsPerFile) {
            this.stringsPerFile = stringsPerFile;
        }
        
        public int getMinStringLength() {
            return minStringLength;
        }
        
        public void setMinStringLength(int minStringLength) {
            this.minStringLength = minStringLength;
        }
        
        public int getMaxStringLength() {
            return maxStringLength;
        }
        
        public void setMaxStringLength(int maxStringLength) {
            this.maxStringLength = maxStringLength;
        }
        
        public boolean isVerifyResults() {
            return verifyResults;
        }
        
        public void setVerifyResults(boolean verifyResults) {
            this.verifyResults = verifyResults;
        }
        
        public boolean isGenerateReport() {
            return generateReport;
        }
        
        public void setGenerateReport(boolean generateReport) {
            this.generateReport = generateReport;
        }
    }
    
    /**
     * 测试结果类
     */
    public static class TestResult {
        private long startTime;
        private long endTime;
        private long totalTestTimeMs;
        private int totalTasks;
        private int completedTasks;
        private int failedTasks;
        private long minProcessingTimeMs;
        private long maxProcessingTimeMs;
        private long avgProcessingTimeMs;
        private long medianProcessingTimeMs;
        private long p95ProcessingTimeMs;
        
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
        
        public int getTotalTasks() {
            return totalTasks;
        }
        
        public void setTotalTasks(int totalTasks) {
            this.totalTasks = totalTasks;
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
        
        /**
         * 计算每秒处理的任务数
         *
         * @return 每秒处理的任务数
         */
        public double getThroughputPerSecond() {
            if (totalTestTimeMs > 0) {
                return completedTasks * 1000.0 / totalTestTimeMs;
            }
            return 0;
        }
        
        /**
         * 获取任务成功率
         *
         * @return 成功率
         */
        public double getSuccessRate() {
            if (totalTasks > 0) {
                return (double) completedTasks / totalTasks;
            }
            return 0;
        }
    }
} 