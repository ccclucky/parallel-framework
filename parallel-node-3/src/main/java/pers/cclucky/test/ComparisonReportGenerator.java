package pers.cclucky.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 对比测试报告生成器
 * 用于生成单线程和并行框架的性能对比报告
 */
public class ComparisonReportGenerator {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComparisonReportGenerator.class);
    
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 生成HTML格式的性能对比报告
     * 
     * @param parallelResult 并行框架测试结果
     * @param singleThreadResult 单线程测试结果
     * @param config 测试配置
     * @param reportDir 报告输出目录
     * @throws IOException 如果文件操作失败
     */
    public static void generateHtmlReport(
            ParallelPerformanceTest.TestResult parallelResult,
            ParallelPerformanceTest.TestResult singleThreadResult,
            ParallelPerformanceTest.TestConfig config,
            String reportDir) throws IOException {
        
        File dir = new File(reportDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        String reportPath = new File(dir, "comparison_report_" + timestamp + ".html").getPath();
        
        logger.info("生成性能对比报告: {}", reportPath);
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportPath))) {
            writer.write("<!DOCTYPE html>\n");
            writer.write("<html lang=\"zh-CN\">\n");
            writer.write("<head>\n");
            writer.write("    <meta charset=\"UTF-8\">\n");
            writer.write("    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n");
            writer.write("    <title>并行框架性能对比测试报告</title>\n");
            writer.write("    <style>\n");
            writer.write("        body { font-family: 'Microsoft YaHei', Arial, sans-serif; line-height: 1.6; margin: 0; padding: 20px; color: #333; }\n");
            writer.write("        .container { max-width: 1200px; margin: 0 auto; background-color: #fff; padding: 20px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }\n");
            writer.write("        h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }\n");
            writer.write("        h2 { color: #2980b9; margin-top: 20px; }\n");
            writer.write("        table { width: 100%; border-collapse: collapse; margin: 20px 0; }\n");
            writer.write("        th, td { padding: 12px 15px; border: 1px solid #ddd; text-align: left; }\n");
            writer.write("        th { background-color: #3498db; color: white; }\n");
            writer.write("        tr:nth-child(even) { background-color: #f2f2f2; }\n");
            writer.write("        .improvement { color: #27ae60; font-weight: bold; }\n");
            writer.write("        .degradation { color: #e74c3c; font-weight: bold; }\n");
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
            writer.write("        <h1>并行框架与单线程字符串反转性能对比报告</h1>\n");
            
            // 测试概述
            writer.write("        <h2>测试配置</h2>\n");
            writer.write("        <table>\n");
            writer.write("            <tr><th>配置项</th><th>值</th></tr>\n");
            writer.write("            <tr><td>测试数据目录</td><td>" + config.getDataDir() + "</td></tr>\n");
            writer.write("            <tr><td>测试开始时间</td><td>" + DATE_FORMAT.format(new Date(parallelResult.getStartTime())) + "</td></tr>\n");
            writer.write("            <tr><td>测试结束时间</td><td>" + DATE_FORMAT.format(new Date(parallelResult.getEndTime())) + "</td></tr>\n");
            writer.write("            <tr><td>总任务数</td><td>" + parallelResult.getTotalTasks() + "</td></tr>\n");
            writer.write("            <tr><td>并行任务数</td><td>" + config.getConcurrentTasks() + "</td></tr>\n");
            writer.write("            <tr><td>超时时间</td><td>" + config.getTimeoutMinutes() + " 分钟</td></tr>\n");
            writer.write("            <tr><td>字符串最小长度</td><td>" + config.getMinStringLength() + " 字符</td></tr>\n");
            writer.write("            <tr><td>字符串最大长度</td><td>" + config.getMaxStringLength() + " 字符</td></tr>\n");
            writer.write("            <tr><td>验证结果</td><td>" + (config.isVerifyResults() ? "是" : "否") + "</td></tr>\n");
            writer.write("        </table>\n");
            
            // 任务统计对比
            writer.write("        <h2>任务统计对比</h2>\n");
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>并行框架</th><th>单线程</th><th>对比</th></tr>\n");
            
            int parallelCompleted = parallelResult.getCompletedTasks();
            int singleCompleted = singleThreadResult.getCompletedTasks();
            int parallelTotal = parallelResult.getTotalTasks();
            int singleTotal = singleThreadResult.getTotalTasks();
            double completedImprovement = calculateImprovement(
                    (double)singleCompleted / singleTotal,
                    (double)parallelCompleted / parallelTotal);
            String completedColor = completedImprovement > 0 ? "improvement" : "degradation";
            
            writer.write("            <tr><td>成功任务数</td>");
            writer.write("<td>" + parallelCompleted + " (" + formatPercent(parallelCompleted, parallelTotal) + ")</td>");
            writer.write("<td>" + singleCompleted + " (" + formatPercent(singleCompleted, singleTotal) + ")</td>");
            writer.write("<td class=\"" + completedColor + "\">" + String.format("%.2f%%", completedImprovement) + "</td></tr>\n");
            
            int parallelFailed = parallelResult.getFailedTasks();
            int singleFailed = singleThreadResult.getFailedTasks();
            double failedImprovement = calculateImprovement(
                    (double)singleFailed / singleTotal, 
                    (double)parallelFailed / parallelTotal);
            String failedColor = failedImprovement > 0 ? "improvement" : "degradation";
            
            writer.write("            <tr><td>失败任务数</td>");
            writer.write("<td>" + parallelFailed + " (" + formatPercent(parallelFailed, parallelTotal) + ")</td>");
            writer.write("<td>" + singleFailed + " (" + formatPercent(singleFailed, singleTotal) + ")</td>");
            writer.write("<td class=\"" + failedColor + "\">" + String.format("%.2f%%", failedImprovement) + "</td></tr>\n");
            
            writer.write("        </table>\n");
            
            // 性能指标对比
            writer.write("        <h2>性能指标对比</h2>\n");
            
            // 摘要指标：成功率和吞吐量
            double parallelSuccessRate = parallelResult.getSuccessRate() * 100;
            double singleSuccessRate = singleThreadResult.getSuccessRate() * 100;
            double successRateImprovement = calculateImprovement(singleSuccessRate, parallelSuccessRate);
            String successRateColor = successRateImprovement > 0 ? "improvement" : "degradation";
            
            writer.write("        <div class=\"success-rate\">成功率对比: ");
            writer.write("<span class=\"rate\">并行框架 " + String.format("%.2f%%", parallelSuccessRate) + "</span> vs ");
            writer.write("<span class=\"rate\">单线程 " + String.format("%.2f%%", singleSuccessRate) + "</span> ");
            writer.write("<span class=\"" + successRateColor + "\">(" + (successRateImprovement > 0 ? "+" : "") 
                    + String.format("%.2f%%", successRateImprovement) + ")</span></div>\n");
            
            double parallelThroughput = parallelResult.getThroughputPerSecond();
            double singleThroughput = singleThreadResult.getThroughputPerSecond();
            double throughputImprovement = calculateThroughputImprovement(parallelThroughput, singleThroughput);
            String throughputColor = throughputImprovement > 0 ? "improvement" : "degradation";
            
            writer.write("        <div class=\"throughput\">吞吐量对比: ");
            writer.write("<span class=\"rate\">并行框架 " + String.format("%.2f", parallelThroughput) + " 任务/秒</span> vs ");
            writer.write("<span class=\"rate\">单线程 " + String.format("%.2f", singleThroughput) + " 任务/秒</span> ");
            writer.write("<span class=\"" + throughputColor + "\">(" + (throughputImprovement > 0 ? "+" : "") 
                    + String.format("%.2f%%", throughputImprovement) + ")</span></div>\n");
            
            // 详细指标表格
            writer.write("        <table>\n");
            writer.write("            <tr><th>指标</th><th>并行框架</th><th>单线程</th><th>提升比例</th></tr>\n");
            
            // 吞吐量对比（已在上面显示，这里再次显示在表格中）
            writer.write("            <tr><td>吞吐量 (任务/秒)</td>");
            writer.write("<td>" + String.format("%.2f", parallelThroughput) + "</td>");
            writer.write("<td>" + String.format("%.2f", singleThroughput) + "</td>");
            writer.write("<td class=\"" + throughputColor + "\">" + String.format("%.2f%%", throughputImprovement) + "</td></tr>\n");
            
            // 总处理时间对比
            long parallelTotalTime = parallelResult.getTotalTestTimeMs();
            long singleTotalTime = singleThreadResult.getTotalTestTimeMs();
            double totalTimeImprovement = calculateTimeImprovement(singleTotalTime, parallelTotalTime);
            String totalTimeColor = totalTimeImprovement > 0 ? "improvement" : "degradation";
            
            writer.write("            <tr><td>总处理时间</td>");
            writer.write("<td>" + formatTime(parallelTotalTime) + "</td>");
            writer.write("<td>" + formatTime(singleTotalTime) + "</td>");
            writer.write("<td class=\"" + totalTimeColor + "\">" + String.format("%.2f%%", totalTimeImprovement) + "</td></tr>\n");
            
            if (parallelResult.getCompletedTasks() > 0 && singleThreadResult.getCompletedTasks() > 0) {
                // 最小处理时间
                long parallelMinTime = parallelResult.getMinProcessingTimeMs();
                long singleMinTime = singleThreadResult.getMinProcessingTimeMs();
                double minTimeImprovement = calculateTimeImprovement(singleMinTime, parallelMinTime);
                String minTimeColor = minTimeImprovement > 0 ? "improvement" : "degradation";
                
                writer.write("            <tr><td>最小处理时间</td>");
                writer.write("<td>" + formatTime(parallelMinTime) + "</td>");
                writer.write("<td>" + formatTime(singleMinTime) + "</td>");
                writer.write("<td class=\"" + minTimeColor + "\">" + String.format("%.2f%%", minTimeImprovement) + "</td></tr>\n");
                
                // 最大处理时间
                long parallelMaxTime = parallelResult.getMaxProcessingTimeMs();
                long singleMaxTime = singleThreadResult.getMaxProcessingTimeMs();
                double maxTimeImprovement = calculateTimeImprovement(singleMaxTime, parallelMaxTime);
                String maxTimeColor = maxTimeImprovement > 0 ? "improvement" : "degradation";
                
                writer.write("            <tr><td>最大处理时间</td>");
                writer.write("<td>" + formatTime(parallelMaxTime) + "</td>");
                writer.write("<td>" + formatTime(singleMaxTime) + "</td>");
                writer.write("<td class=\"" + maxTimeColor + "\">" + String.format("%.2f%%", maxTimeImprovement) + "</td></tr>\n");
                
                // 平均处理时间
                long parallelAvgTime = parallelResult.getAvgProcessingTimeMs();
                long singleAvgTime = singleThreadResult.getAvgProcessingTimeMs();
                double avgTimeImprovement = calculateTimeImprovement(singleAvgTime, parallelAvgTime);
                String avgTimeColor = avgTimeImprovement > 0 ? "improvement" : "degradation";
                
                writer.write("            <tr><td>平均处理时间</td>");
                writer.write("<td>" + formatTime(parallelAvgTime) + "</td>");
                writer.write("<td>" + formatTime(singleAvgTime) + "</td>");
                writer.write("<td class=\"" + avgTimeColor + "\">" + String.format("%.2f%%", avgTimeImprovement) + "</td></tr>\n");
                
                // 中位数处理时间
                long parallelMedianTime = parallelResult.getMedianProcessingTimeMs();
                long singleMedianTime = singleThreadResult.getMedianProcessingTimeMs();
                double medianTimeImprovement = calculateTimeImprovement(singleMedianTime, parallelMedianTime);
                String medianTimeColor = medianTimeImprovement > 0 ? "improvement" : "degradation";
                
                writer.write("            <tr><td>中位数处理时间</td>");
                writer.write("<td>" + formatTime(parallelMedianTime) + "</td>");
                writer.write("<td>" + formatTime(singleMedianTime) + "</td>");
                writer.write("<td class=\"" + medianTimeColor + "\">" + String.format("%.2f%%", medianTimeImprovement) + "</td></tr>\n");
                
                // 95%分位处理时间
                long parallelP95Time = parallelResult.getP95ProcessingTimeMs();
                long singleP95Time = singleThreadResult.getP95ProcessingTimeMs();
                double p95TimeImprovement = calculateTimeImprovement(singleP95Time, parallelP95Time);
                String p95TimeColor = p95TimeImprovement > 0 ? "improvement" : "degradation";
                
                writer.write("            <tr><td>95%分位处理时间</td>");
                writer.write("<td>" + formatTime(parallelP95Time) + "</td>");
                writer.write("<td>" + formatTime(singleP95Time) + "</td>");
                writer.write("<td class=\"" + p95TimeColor + "\">" + String.format("%.2f%%", p95TimeImprovement) + "</td></tr>\n");
            }
            
            writer.write("        </table>\n");
            
            // 图表
            writer.write("        <h2>性能对比图表</h2>\n");
            writer.write("        <div class=\"chart-container\">\n");
            writer.write("            <canvas id=\"taskDistributionChart\"></canvas>\n");
            writer.write("        </div>\n");
            
            writer.write("        <div class=\"chart-container\">\n");
            writer.write("            <canvas id=\"throughputChart\"></canvas>\n");
            writer.write("        </div>\n");
            
            writer.write("        <div class=\"chart-container\">\n");
            writer.write("            <canvas id=\"timeChart\"></canvas>\n");
            writer.write("        </div>\n");
            
            // JavaScript图表
            writer.write("        <script>\n");
            
            // 任务分布图表
            writer.write("            // 任务分布图表\n");
            writer.write("            var taskDistCtx = document.getElementById('taskDistributionChart').getContext('2d');\n");
            writer.write("            new Chart(taskDistCtx, {\n");
            writer.write("                type: 'bar',\n");
            writer.write("                data: {\n");
            writer.write("                    labels: ['成功完成', '失败'],\n");
            writer.write("                    datasets: [{\n");
            writer.write("                        label: '并行框架',\n");
            writer.write("                        data: [" + 
                       parallelResult.getCompletedTasks() + ", " + 
                       parallelResult.getFailedTasks() + "],\n");
            writer.write("                        backgroundColor: '#3498db'\n");
            writer.write("                    }, {\n");
            writer.write("                        label: '单线程',\n");
            writer.write("                        data: [" + 
                       singleThreadResult.getCompletedTasks() + ", " + 
                       singleThreadResult.getFailedTasks() + "],\n");
            writer.write("                        backgroundColor: '#e67e22'\n");
            writer.write("                    }]\n");
            writer.write("                },\n");
            writer.write("                options: {\n");
            writer.write("                    responsive: true,\n");
            writer.write("                    plugins: {\n");
            writer.write("                        title: {\n");
            writer.write("                            display: true,\n");
            writer.write("                            text: '任务执行结果分布对比'\n");
            writer.write("                        }\n");
            writer.write("                    },\n");
            writer.write("                    scales: {\n");
            writer.write("                        y: {\n");
            writer.write("                            beginAtZero: true\n");
            writer.write("                        }\n");
            writer.write("                    }\n");
            writer.write("                }\n");
            writer.write("            });\n");
            
            // 吞吐量对比图表
            writer.write("            // 吞吐量对比图表\n");
            writer.write("            var ctx1 = document.getElementById('throughputChart').getContext('2d');\n");
            writer.write("            new Chart(ctx1, {\n");
            writer.write("                type: 'bar',\n");
            writer.write("                data: {\n");
            writer.write("                    labels: ['并行框架', '单线程'],\n");
            writer.write("                    datasets: [{\n");
            writer.write("                        label: '吞吐量 (任务/秒)',\n");
            writer.write("                        data: [" + parallelThroughput + ", " + singleThroughput + "],\n");
            writer.write("                        backgroundColor: ['#3498db', '#e67e22']\n");
            writer.write("                    }]\n");
            writer.write("                },\n");
            writer.write("                options: {\n");
            writer.write("                    responsive: true,\n");
            writer.write("                    plugins: {\n");
            writer.write("                        title: {\n");
            writer.write("                            display: true,\n");
            writer.write("                            text: '吞吐量对比 (越高越好)'\n");
            writer.write("                        },\n");
            writer.write("                    },\n");
            writer.write("                    scales: {\n");
            writer.write("                        y: {\n");
            writer.write("                            beginAtZero: true\n");
            writer.write("                        }\n");
            writer.write("                    }\n");
            writer.write("                }\n");
            writer.write("            });\n");
            
            // 处理时间对比图表
            writer.write("            // 处理时间对比图表\n");
            writer.write("            var ctx2 = document.getElementById('timeChart').getContext('2d');\n");
            writer.write("            new Chart(ctx2, {\n");
            writer.write("                type: 'bar',\n");
            writer.write("                data: {\n");
            writer.write("                    labels: ['最小处理时间', '平均处理时间', '中位数处理时间', '95%分位处理时间', '最大处理时间'],\n");
            writer.write("                    datasets: [{\n");
            writer.write("                        label: '并行框架 (毫秒)',\n");
            writer.write("                        data: [" + 
                       parallelResult.getMinProcessingTimeMs() + ", " + 
                       parallelResult.getAvgProcessingTimeMs() + ", " + 
                       parallelResult.getMedianProcessingTimeMs() + ", " + 
                       parallelResult.getP95ProcessingTimeMs() + ", " + 
                       parallelResult.getMaxProcessingTimeMs() + "],\n");
            writer.write("                        backgroundColor: '#3498db'\n");
            writer.write("                    }, {\n");
            writer.write("                        label: '单线程 (毫秒)',\n");
            writer.write("                        data: [" + 
                       singleThreadResult.getMinProcessingTimeMs() + ", " + 
                       singleThreadResult.getAvgProcessingTimeMs() + ", " + 
                       singleThreadResult.getMedianProcessingTimeMs() + ", " + 
                       singleThreadResult.getP95ProcessingTimeMs() + ", " + 
                       singleThreadResult.getMaxProcessingTimeMs() + "],\n");
            writer.write("                        backgroundColor: '#e67e22'\n");
            writer.write("                    }]\n");
            writer.write("                },\n");
            writer.write("                options: {\n");
            writer.write("                    responsive: true,\n");
            writer.write("                    plugins: {\n");
            writer.write("                        title: {\n");
            writer.write("                            display: true,\n");
            writer.write("                            text: '处理时间对比 (越低越好)'\n");
            writer.write("                        },\n");
            writer.write("                    },\n");
            writer.write("                    scales: {\n");
            writer.write("                        y: {\n");
            writer.write("                            beginAtZero: true\n");
            writer.write("                        }\n");
            writer.write("                    }\n");
            writer.write("                }\n");
            writer.write("            });\n");
            writer.write("        </script>\n");
            
            // 摘要结论
            writer.write("        <h2>性能测试摘要</h2>\n");
            writer.write("        <div style=\"padding: 20px; background-color: #f9f9f9; border-left: 5px solid #3498db;\">\n");
            
            if (throughputImprovement > 0) {
                writer.write("            <p>并行框架相比单线程处理提升了<span class=\"improvement\"> " 
                        + String.format("%.2f", throughputImprovement) + "% </span>的吞吐量。</p>\n");
            } else {
                writer.write("            <p>并行框架相比单线程处理降低了<span class=\"degradation\"> " 
                        + String.format("%.2f", -throughputImprovement) + "% </span>的吞吐量。</p>\n");
            }
            
            if (totalTimeImprovement > 0) {
                writer.write("            <p>并行框架相比单线程处理节省了<span class=\"improvement\"> "
                        + String.format("%.2f", totalTimeImprovement) + "% </span>的总处理时间。</p>\n");
            } else {
                writer.write("            <p>并行框架相比单线程处理增加了<span class=\"degradation\"> "
                        + String.format("%.2f", -totalTimeImprovement) + "% </span>的总处理时间。</p>\n");
            }
            
            if (parallelResult.getCompletedTasks() > 0 && singleThreadResult.getCompletedTasks() > 0) {
                long parallelAvgTime = parallelResult.getAvgProcessingTimeMs();
                long singleAvgTime = singleThreadResult.getAvgProcessingTimeMs();
                double avgTimeImprovement = calculateTimeImprovement(singleAvgTime, parallelAvgTime);
                
                if (avgTimeImprovement > 0) {
                    writer.write("            <p>并行框架相比单线程处理减少了<span class=\"improvement\"> "
                            + String.format("%.2f", avgTimeImprovement) + "% </span>的平均处理时间。</p>\n");
                } else {
                    writer.write("            <p>并行框架相比单线程处理增加了<span class=\"degradation\"> "
                            + String.format("%.2f", -avgTimeImprovement) + "% </span>的平均处理时间。</p>\n");
                }
            }
            
            writer.write("        </div>\n");
            
            // 报告页脚
            writer.write("        <div style=\"margin-top: 50px; text-align: center; color: #7f8c8d; font-size: 12px;\">\n");
            writer.write("            <p>报告生成时间: " + DATE_FORMAT.format(new Date()) + "</p>\n");
            writer.write("            <p>并行计算框架性能测试工具</p>\n");
            writer.write("        </div>\n");
            
            writer.write("    </div>\n");
            writer.write("</body>\n");
            writer.write("</html>\n");
        }
        
        logger.info("性能对比报告已生成: {}", reportPath);
    }
    
    /**
     * 生成文本格式的性能对比报告
     * 
     * @param parallelResult 并行框架测试结果
     * @param singleThreadResult 单线程测试结果
     * @param config 测试配置
     * @param reportDir 报告输出目录
     * @throws IOException 如果文件操作失败
     */
    public static void generateTextReport(
            ParallelPerformanceTest.TestResult parallelResult,
            ParallelPerformanceTest.TestResult singleThreadResult,
            ParallelPerformanceTest.TestConfig config,
            String reportDir) throws IOException {
        
        File dir = new File(reportDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        String reportPath = new File(dir, "comparison_report_" + timestamp + ".txt").getPath();
        
        logger.info("生成文本格式性能对比报告: {}", reportPath);
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportPath))) {
            writer.write("=========================================================\n");
            writer.write("        并行框架与单线程字符串反转性能对比报告          \n");
            writer.write("=========================================================\n\n");
            
            // 测试配置
            writer.write("测试配置:\n");
            writer.write("---------------------------------------------------------\n");
            writer.write(String.format("测试数据目录: %s\n", config.getDataDir()));
            writer.write(String.format("测试开始时间: %s\n", DATE_FORMAT.format(new Date(parallelResult.getStartTime()))));
            writer.write(String.format("测试结束时间: %s\n", DATE_FORMAT.format(new Date(parallelResult.getEndTime()))));
            writer.write(String.format("总任务数: %d\n", parallelResult.getTotalTasks()));
            writer.write(String.format("并行任务数: %d\n", config.getConcurrentTasks()));
            writer.write(String.format("超时时间: %d 分钟\n", config.getTimeoutMinutes()));
            writer.write(String.format("字符串最小长度: %d 字符\n", config.getMinStringLength()));
            writer.write(String.format("字符串最大长度: %d 字符\n", config.getMaxStringLength()));
            writer.write(String.format("验证结果: %s\n", config.isVerifyResults() ? "是" : "否"));
            writer.write("\n");
            
            // 任务统计对比
            writer.write("任务统计对比:\n");
            writer.write("---------------------------------------------------------\n");
            
            int parallelCompleted = parallelResult.getCompletedTasks();
            int singleCompleted = singleThreadResult.getCompletedTasks();
            int parallelTotal = parallelResult.getTotalTasks();
            int singleTotal = singleThreadResult.getTotalTasks();
            double completedImprovement = calculateImprovement(
                    (double)singleCompleted / singleTotal,
                    (double)parallelCompleted / parallelTotal);
            
            writer.write(String.format("成功任务数: 并行框架 %d (%s) vs 单线程 %d (%s), 变化: %s%.2f%%\n", 
                    parallelCompleted, formatPercent(parallelCompleted, parallelTotal),
                    singleCompleted, formatPercent(singleCompleted, singleTotal),
                    completedImprovement > 0 ? "+" : "", completedImprovement));
            
            int parallelFailed = parallelResult.getFailedTasks();
            int singleFailed = singleThreadResult.getFailedTasks();
            double failedImprovement = calculateImprovement(
                    (double)singleFailed / singleTotal, 
                    (double)parallelFailed / parallelTotal);
            
            writer.write(String.format("失败任务数: 并行框架 %d (%s) vs 单线程 %d (%s), 变化: %s%.2f%%\n", 
                    parallelFailed, formatPercent(parallelFailed, parallelTotal),
                    singleFailed, formatPercent(singleFailed, singleTotal),
                    failedImprovement > 0 ? "+" : "", failedImprovement));
            
            writer.write("\n");
            
            // 性能指标对比
            writer.write("性能指标对比:\n");
            writer.write("---------------------------------------------------------\n");
            
            double parallelSuccessRate = parallelResult.getSuccessRate() * 100;
            double singleSuccessRate = singleThreadResult.getSuccessRate() * 100;
            double successRateImprovement = calculateImprovement(singleSuccessRate, parallelSuccessRate);
            
            writer.write(String.format("成功率: 并行框架 %.2f%% vs 单线程 %.2f%%, 变化: %s%.2f%%\n", 
                    parallelSuccessRate, singleSuccessRate,
                    successRateImprovement > 0 ? "+" : "", successRateImprovement));
            
            double parallelThroughput = parallelResult.getThroughputPerSecond();
            double singleThroughput = singleThreadResult.getThroughputPerSecond();
            double throughputImprovement = calculateThroughputImprovement(parallelThroughput, singleThroughput);
            
            writer.write(String.format("吞吐量: 并行框架 %.2f 任务/秒 vs 单线程 %.2f 任务/秒, 变化: %s%.2f%%\n", 
                    parallelThroughput, singleThroughput,
                    throughputImprovement > 0 ? "+" : "", throughputImprovement));
            
            long parallelTotalTime = parallelResult.getTotalTestTimeMs();
            long singleTotalTime = singleThreadResult.getTotalTestTimeMs();
            double totalTimeImprovement = calculateTimeImprovement(singleTotalTime, parallelTotalTime);
            
            writer.write(String.format("总处理时间: 并行框架 %s vs 单线程 %s, 变化: %s%.2f%%\n", 
                    formatTime(parallelTotalTime), formatTime(singleTotalTime),
                    totalTimeImprovement > 0 ? "+" : "", totalTimeImprovement));
            
            if (parallelResult.getCompletedTasks() > 0 && singleThreadResult.getCompletedTasks() > 0) {
                // 最小处理时间
                long parallelMinTime = parallelResult.getMinProcessingTimeMs();
                long singleMinTime = singleThreadResult.getMinProcessingTimeMs();
                double minTimeImprovement = calculateTimeImprovement(singleMinTime, parallelMinTime);
                
                writer.write(String.format("最小处理时间: 并行框架 %s vs 单线程 %s, 变化: %s%.2f%%\n", 
                        formatTime(parallelMinTime), formatTime(singleMinTime),
                        minTimeImprovement > 0 ? "+" : "", minTimeImprovement));
                
                // 最大处理时间
                long parallelMaxTime = parallelResult.getMaxProcessingTimeMs();
                long singleMaxTime = singleThreadResult.getMaxProcessingTimeMs();
                double maxTimeImprovement = calculateTimeImprovement(singleMaxTime, parallelMaxTime);
                
                writer.write(String.format("最大处理时间: 并行框架 %s vs 单线程 %s, 变化: %s%.2f%%\n", 
                        formatTime(parallelMaxTime), formatTime(singleMaxTime),
                        maxTimeImprovement > 0 ? "+" : "", maxTimeImprovement));
                
                // 平均处理时间
                long parallelAvgTime = parallelResult.getAvgProcessingTimeMs();
                long singleAvgTime = singleThreadResult.getAvgProcessingTimeMs();
                double avgTimeImprovement = calculateTimeImprovement(singleAvgTime, parallelAvgTime);
                
                writer.write(String.format("平均处理时间: 并行框架 %s vs 单线程 %s, 变化: %s%.2f%%\n", 
                        formatTime(parallelAvgTime), formatTime(singleAvgTime),
                        avgTimeImprovement > 0 ? "+" : "", avgTimeImprovement));
                
                // 中位数处理时间
                long parallelMedianTime = parallelResult.getMedianProcessingTimeMs();
                long singleMedianTime = singleThreadResult.getMedianProcessingTimeMs();
                double medianTimeImprovement = calculateTimeImprovement(singleMedianTime, parallelMedianTime);
                
                writer.write(String.format("中位数处理时间: 并行框架 %s vs 单线程 %s, 变化: %s%.2f%%\n", 
                        formatTime(parallelMedianTime), formatTime(singleMedianTime),
                        medianTimeImprovement > 0 ? "+" : "", medianTimeImprovement));
                
                // 95%分位处理时间
                long parallelP95Time = parallelResult.getP95ProcessingTimeMs();
                long singleP95Time = singleThreadResult.getP95ProcessingTimeMs();
                double p95TimeImprovement = calculateTimeImprovement(singleP95Time, parallelP95Time);
                
                writer.write(String.format("95%%分位处理时间: 并行框架 %s vs 单线程 %s, 变化: %s%.2f%%\n", 
                        formatTime(parallelP95Time), formatTime(singleP95Time),
                        p95TimeImprovement > 0 ? "+" : "", p95TimeImprovement));
            }
            
            writer.write("\n");
            
            // 摘要结论
            writer.write("性能测试摘要:\n");
            writer.write("---------------------------------------------------------\n");
            
            if (throughputImprovement > 0) {
                writer.write(String.format("并行框架相比单线程处理提升了 %.2f%% 的吞吐量。\n", throughputImprovement));
            } else {
                writer.write(String.format("并行框架相比单线程处理降低了 %.2f%% 的吞吐量。\n", -throughputImprovement));
            }
            
            if (totalTimeImprovement > 0) {
                writer.write(String.format("并行框架相比单线程处理节省了 %.2f%% 的总处理时间。\n", totalTimeImprovement));
            } else {
                writer.write(String.format("并行框架相比单线程处理增加了 %.2f%% 的总处理时间。\n", -totalTimeImprovement));
            }
            
            writer.write("\n");
            writer.write(String.format("报告生成时间: %s\n", DATE_FORMAT.format(new Date())));
            writer.write("=========================================================\n");
        }
        
        logger.info("文本格式性能对比报告已生成: {}", reportPath);
    }
    
    /**
     * 计算改进百分比
     * @param newValue 新值
     * @param oldValue 旧值
     * @return 改进百分比
     */
    private static double calculateImprovement(double newValue, double oldValue) {
        if (oldValue == 0) {
            // 防止除零
            return newValue > 0 ? 100 : 0;
        }
        // 计算提升百分比：(新 - 旧) / 旧 * 100
        return ((newValue - oldValue) / oldValue) * 100;
    }
    
    /**
     * 计算时间差异百分比
     * @param oldTimeMs 旧时间（毫秒）
     * @param newTimeMs 新时间（毫秒）
     * @return 时间减少的百分比（正值表示减少，负值表示增加）
     */
    private static double calculateTimeImprovement(long oldTimeMs, long newTimeMs) {
        if (oldTimeMs == 0) {
            // 防止除零
            return newTimeMs < oldTimeMs ? 100 : -100;
        }
        return ((double)(oldTimeMs - newTimeMs) / oldTimeMs) * 100;
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
     * 计算吞吐量提升百分比
     * @param newThroughput 新吞吐量
     * @param oldThroughput 旧吞吐量
     * @return 提升百分比
     */
    private static double calculateThroughputImprovement(double newThroughput, double oldThroughput) {
        if (oldThroughput == 0) {
            return newThroughput > 0 ? 100 : 0;
        }
        // 计算提升百分比：(新 - 旧) / 旧 * 100
        return ((newThroughput - oldThroughput) / oldThroughput) * 100;
    }
} 