package pers.cclucky.parallel.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 测试报告生成器
 */
public class TestReportGenerator {
    
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 生成HTML格式的测试报告
     * 
     * @param result 测试结果
     * @param outputPath 输出路径
     * @param testConfig 测试配置信息
     * @throws IOException 如果文件操作失败
     */
    public static void generateHtmlReport(PerformanceTestClient.TestResult result, String outputPath, TestConfig testConfig) throws IOException {
        File outputFile = new File(outputPath);
        
        // 确保目录存在
        File parentDir = outputFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("<!DOCTYPE html>\n");
            writer.write("<html lang=\"zh-CN\">\n");
            writer.write("<head>\n");
            writer.write("    <meta charset=\"UTF-8\">\n");
            writer.write("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
            writer.write("    <title>并行计算框架性能测试报告</title>\n");
            writer.write("    <style>\n");
            writer.write("        body { font-family: 'Microsoft YaHei', Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; color: #333; }\n");
            writer.write("        .container { max-width: 1200px; margin: 0 auto; background-color: #fff; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }\n");
            writer.write("        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }\n");
            writer.write("        h2 { color: #2980b9; margin-top: 20px; }\n");
            writer.write("        table { width: 100%; border-collapse: collapse; margin: 20px 0; }\n");
            writer.write("        th, td { padding: 12px 15px; border: 1px solid #ddd; text-align: left; }\n");
            writer.write("        th { background-color: #3498db; color: white; }\n");
            writer.write("        tr:nth-child(even) { background-color: #f2f2f2; }\n");
            writer.write("        .metric { font-weight: bold; }\n");
            writer.write("        .chart-container { width: 100%; height: 400px; margin: 20px 0; }\n");
            writer.write("        .success-rate { font-size: 24px; font-weight: bold; margin: 20px 0; }\n");
            writer.write("        .success-rate .rate { color: #27ae60; }\n");
            writer.write("        .throughput { font-size: 24px; font-weight: bold; margin: 20px 0; }\n");
            writer.write("        .throughput .rate { color: #2980b9; }\n");
            writer.write("    </style>\n");
            writer.write("    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n");
            writer.write("</head>\n");
            writer.write("<body>\n");
            writer.write("    <div class=\"container\">\n");
            writer.write("        <h1>并行计算框架性能测试报告</h1>\n");
            
            // 测试概述
            writer.write("        <h2>测试概述</h2>\n");
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>值</th></tr>\n");
            writer.write("            <tr><td>测试开始时间</td><td>" + DATE_FORMAT.format(new Date(result.getStartTime())) + "</td></tr>\n");
            writer.write("            <tr><td>测试结束时间</td><td>" + DATE_FORMAT.format(new Date(result.getEndTime())) + "</td></tr>\n");
            writer.write("            <tr><td>测试总耗时</td><td>" + formatTime(result.getTotalTestTimeMs()) + "</td></tr>\n");
            writer.write("            <tr><td>节点数量</td><td>" + testConfig.getNodeCount() + "</td></tr>\n");
            writer.write("            <tr><td>并发任务数</td><td>" + testConfig.getConcurrentTasks() + "</td></tr>\n");
            writer.write("            <tr><td>测试数据目录</td><td>" + testConfig.getTestDataDir() + "</td></tr>\n");
            writer.write("            <tr><td>任务优先级</td><td>" + testConfig.getPriority() + "</td></tr>\n");
            writer.write("            <tr><td>任务超时时间</td><td>" + formatTime(testConfig.getTaskTimeoutMs()) + "</td></tr>\n");
            writer.write("            <tr><td>任务是否可重试</td><td>" + (testConfig.isRetryable() ? "是" : "否") + "</td></tr>\n");
            writer.write("            <tr><td>最大重试次数</td><td>" + testConfig.getMaxRetryCount() + "</td></tr>\n");
            writer.write("        </table>\n");
            
            // 任务统计
            writer.write("        <h2>任务统计</h2>\n");
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>数量</th><th>百分比</th></tr>\n");
            int totalTasks = result.getSubmittedTasks();
            writer.write("            <tr><td>提交任务总数</td><td>" + totalTasks + "</td><td>100%</td></tr>\n");
            writer.write("            <tr><td>成功完成任务数</td><td>" + result.getCompletedTasks() + "</td><td>" 
                    + formatPercent(result.getCompletedTasks(), totalTasks) + "</td></tr>\n");
            writer.write("            <tr><td>失败任务数</td><td>" + result.getFailedTasks() + "</td><td>" 
                    + formatPercent(result.getFailedTasks(), totalTasks) + "</td></tr>\n");
            writer.write("            <tr><td>取消任务数</td><td>" + result.getCancelledTasks() + "</td><td>" 
                    + formatPercent(result.getCancelledTasks(), totalTasks) + "</td></tr>\n");
            writer.write("        </table>\n");
            
            // 性能指标
            writer.write("        <h2>性能指标</h2>\n");
            writer.write("        <div class=\"success-rate\">成功率: <span class=\"rate\">" + String.format("%.2f%%", result.getSuccessRate() * 100) + "</span></div>\n");
            writer.write("        <div class=\"throughput\">吞吐量: <span class=\"rate\">" + String.format("%.2f", result.getThroughputPerSecond()) + " 任务/秒</span></div>\n");
            
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>值</th></tr>\n");
            if (result.getCompletedTasks() > 0) {
                writer.write("            <tr><td>最小处理时间</td><td>" + formatTime(result.getMinProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>最大处理时间</td><td>" + formatTime(result.getMaxProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>平均处理时间</td><td>" + formatTime(result.getAvgProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>中位数处理时间</td><td>" + formatTime(result.getMedianProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>95%分位处理时间</td><td>" + formatTime(result.getP95ProcessingTimeMs()) + "</td></tr>\n");
                writer.write("            <tr><td>99%分位处理时间</td><td>" + formatTime(result.getP99ProcessingTimeMs()) + "</td></tr>\n");
            } else {
                writer.write("            <tr><td colspan=\"2\">没有成功完成的任务，无法计算处理时间统计</td></tr>\n");
            }
            writer.write("        </table>\n");
            
            // 图表
            writer.write("        <h2>结果图表</h2>\n");
            writer.write("        <div class=\"chart-container\">\n");
            writer.write("            <canvas id=\"taskDistributionChart\"></canvas>\n");
            writer.write("        </div>\n");
            
            if (result.getCompletedTasks() > 0) {
                writer.write("        <div class=\"chart-container\">\n");
                writer.write("            <canvas id=\"processingTimeChart\"></canvas>\n");
                writer.write("        </div>\n");
            }
            
            // JavaScript 图表
            writer.write("        <script>\n");
            
            // 任务分布图表
            writer.write("            // 任务分布图表\n");
            writer.write("            var taskDistributionCtx = document.getElementById('taskDistributionChart').getContext('2d');\n");
            writer.write("            var taskDistributionChart = new Chart(taskDistributionCtx, {\n");
            writer.write("                type: 'pie',\n");
            writer.write("                data: {\n");
            writer.write("                    labels: ['成功完成', '失败', '取消'],\n");
            writer.write("                    datasets: [{\n");
            writer.write("                        data: [" + result.getCompletedTasks() + ", " + result.getFailedTasks() + ", " + result.getCancelledTasks() + "],\n");
            writer.write("                        backgroundColor: ['#27ae60', '#c0392b', '#f39c12'],\n");
            writer.write("                        borderWidth: 1\n");
            writer.write("                    }]\n");
            writer.write("                },\n");
            writer.write("                options: {\n");
            writer.write("                    responsive: true,\n");
            writer.write("                    plugins: {\n");
            writer.write("                        title: {\n");
            writer.write("                            display: true,\n");
            writer.write("                            text: '任务执行结果分布'\n");
            writer.write("                        },\n");
            writer.write("                        legend: {\n");
            writer.write("                            position: 'bottom'\n");
            writer.write("                        }\n");
            writer.write("                    }\n");
            writer.write("                }\n");
            writer.write("            });\n");
            
            // 处理时间图表
            if (result.getCompletedTasks() > 0) {
                writer.write("            // 处理时间图表\n");
                writer.write("            var processingTimeCtx = document.getElementById('processingTimeChart').getContext('2d');\n");
                writer.write("            var processingTimeChart = new Chart(processingTimeCtx, {\n");
                writer.write("                type: 'bar',\n");
                writer.write("                data: {\n");
                writer.write("                    labels: ['最小', '平均', '中位数', '95%分位', '99%分位', '最大'],\n");
                writer.write("                    datasets: [{\n");
                writer.write("                        label: '处理时间 (ms)',\n");
                writer.write("                        data: [" + 
                        result.getMinProcessingTimeMs() + ", " + 
                        result.getAvgProcessingTimeMs() + ", " + 
                        result.getMedianProcessingTimeMs() + ", " + 
                        result.getP95ProcessingTimeMs() + ", " + 
                        result.getP99ProcessingTimeMs() + ", " + 
                        result.getMaxProcessingTimeMs() + "],\n");
                writer.write("                        backgroundColor: '#3498db',\n");
                writer.write("                        borderWidth: 1\n");
                writer.write("                    }]\n");
                writer.write("                },\n");
                writer.write("                options: {\n");
                writer.write("                    responsive: true,\n");
                writer.write("                    plugins: {\n");
                writer.write("                        title: {\n");
                writer.write("                            display: true,\n");
                writer.write("                            text: '任务处理时间统计 (ms)'\n");
                writer.write("                        },\n");
                writer.write("                        legend: {\n");
                writer.write("                            display: false\n");
                writer.write("                        }\n");
                writer.write("                    },\n");
                writer.write("                    scales: {\n");
                writer.write("                        y: {\n");
                writer.write("                            beginAtZero: true\n");
                writer.write("                        }\n");
                writer.write("                    }\n");
                writer.write("                }\n");
                writer.write("            });\n");
            }
            
            writer.write("        </script>\n");
            
            // 报告页脚
            writer.write("        <div style=\"margin-top: 50px; text-align: center; color: #7f8c8d; font-size: 12px;\">\n");
            writer.write("            <p>报告生成时间: " + DATE_FORMAT.format(new Date()) + "</p>\n");
            writer.write("            <p>并行计算框架性能测试工具</p>\n");
            writer.write("        </div>\n");
            
            writer.write("    </div>\n");
            writer.write("</body>\n");
            writer.write("</html>\n");
        }
    }
    
    /**
     * 生成文本格式的测试报告
     * 
     * @param result 测试结果
     * @param outputPath 输出路径
     * @param testConfig 测试配置信息
     * @throws IOException 如果文件操作失败
     */
    public static void generateTextReport(PerformanceTestClient.TestResult result, String outputPath, TestConfig testConfig) throws IOException {
        File outputFile = new File(outputPath);
        
        // 确保目录存在
        File parentDir = outputFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("=====================================================\n");
            writer.write("           并行计算框架性能测试报告                  \n");
            writer.write("=====================================================\n\n");
            
            // 测试概述
            writer.write("测试概述:\n");
            writer.write("-----------------------------------------------------\n");
            writer.write(String.format("测试开始时间: %s\n", DATE_FORMAT.format(new Date(result.getStartTime()))));
            writer.write(String.format("测试结束时间: %s\n", DATE_FORMAT.format(new Date(result.getEndTime()))));
            writer.write(String.format("测试总耗时: %s\n", formatTime(result.getTotalTestTimeMs())));
            writer.write(String.format("节点数量: %d\n", testConfig.getNodeCount()));
            writer.write(String.format("并发任务数: %d\n", testConfig.getConcurrentTasks()));
            writer.write(String.format("测试数据目录: %s\n", testConfig.getTestDataDir()));
            writer.write(String.format("任务优先级: %d\n", testConfig.getPriority()));
            writer.write(String.format("任务超时时间: %s\n", formatTime(testConfig.getTaskTimeoutMs())));
            writer.write(String.format("任务是否可重试: %s\n", testConfig.isRetryable() ? "是" : "否"));
            writer.write(String.format("最大重试次数: %d\n", testConfig.getMaxRetryCount()));
            writer.write("\n");
            
            // 任务统计
            writer.write("任务统计:\n");
            writer.write("-----------------------------------------------------\n");
            int totalTasks = result.getSubmittedTasks();
            writer.write(String.format("提交任务总数: %d (100%%)\n", totalTasks));
            writer.write(String.format("成功完成任务数: %d (%s)\n", 
                    result.getCompletedTasks(), formatPercent(result.getCompletedTasks(), totalTasks)));
            writer.write(String.format("失败任务数: %d (%s)\n", 
                    result.getFailedTasks(), formatPercent(result.getFailedTasks(), totalTasks)));
            writer.write(String.format("取消任务数: %d (%s)\n", 
                    result.getCancelledTasks(), formatPercent(result.getCancelledTasks(), totalTasks)));
            writer.write("\n");
            
            // 性能指标
            writer.write("性能指标:\n");
            writer.write("-----------------------------------------------------\n");
            writer.write(String.format("成功率: %.2f%%\n", result.getSuccessRate() * 100));
            writer.write(String.format("吞吐量: %.2f 任务/秒\n", result.getThroughputPerSecond()));
            
            if (result.getCompletedTasks() > 0) {
                writer.write(String.format("最小处理时间: %s\n", formatTime(result.getMinProcessingTimeMs())));
                writer.write(String.format("最大处理时间: %s\n", formatTime(result.getMaxProcessingTimeMs())));
                writer.write(String.format("平均处理时间: %s\n", formatTime(result.getAvgProcessingTimeMs())));
                writer.write(String.format("中位数处理时间: %s\n", formatTime(result.getMedianProcessingTimeMs())));
                writer.write(String.format("95%%分位处理时间: %s\n", formatTime(result.getP95ProcessingTimeMs())));
                writer.write(String.format("99%%分位处理时间: %s\n", formatTime(result.getP99ProcessingTimeMs())));
            } else {
                writer.write("没有成功完成的任务，无法计算处理时间统计\n");
            }
            
            writer.write("\n");
            writer.write(String.format("报告生成时间: %s\n", DATE_FORMAT.format(new Date())));
            writer.write("=====================================================\n");
        }
    }
    
    /**
     * 测试配置信息
     */
    public static class TestConfig {
        private int nodeCount;
        private int concurrentTasks;
        private String testDataDir;
        private int priority;
        private long taskTimeoutMs;
        private boolean retryable;
        private int maxRetryCount;
        
        public int getNodeCount() {
            return nodeCount;
        }
        
        public void setNodeCount(int nodeCount) {
            this.nodeCount = nodeCount;
        }
        
        public int getConcurrentTasks() {
            return concurrentTasks;
        }
        
        public void setConcurrentTasks(int concurrentTasks) {
            this.concurrentTasks = concurrentTasks;
        }
        
        public String getTestDataDir() {
            return testDataDir;
        }
        
        public void setTestDataDir(String testDataDir) {
            this.testDataDir = testDataDir;
        }
        
        public int getPriority() {
            return priority;
        }
        
        public void setPriority(int priority) {
            this.priority = priority;
        }
        
        public long getTaskTimeoutMs() {
            return taskTimeoutMs;
        }
        
        public void setTaskTimeoutMs(long taskTimeoutMs) {
            this.taskTimeoutMs = taskTimeoutMs;
        }
        
        public boolean isRetryable() {
            return retryable;
        }
        
        public void setRetryable(boolean retryable) {
            this.retryable = retryable;
        }
        
        public int getMaxRetryCount() {
            return maxRetryCount;
        }
        
        public void setMaxRetryCount(int maxRetryCount) {
            this.maxRetryCount = maxRetryCount;
        }
    }
    
    /**
     * 格式化百分比
     */
    private static String formatPercent(int value, int total) {
        if (total == 0) {
            return "0.00%";
        }
        return String.format("%.2f%%", (value * 100.0) / total);
    }
    
    /**
     * 格式化时间
     */
    private static String formatTime(long timeMs) {
        if (timeMs < 1000) {
            return timeMs + " ms";
        } else if (timeMs < 60000) {
            return String.format("%.2f 秒", timeMs / 1000.0);
        } else if (timeMs < 3600000) {
            return String.format("%.2f 分钟", timeMs / 60000.0);
        } else {
            return String.format("%.2f 小时", timeMs / 3600000.0);
        }
    }
} 