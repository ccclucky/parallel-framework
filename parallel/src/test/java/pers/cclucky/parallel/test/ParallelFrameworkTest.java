package pers.cclucky.parallel.test;

import pers.cclucky.parallel.ParallelFramework;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * 并行计算框架测试主类
 */
public class ParallelFrameworkTest {
    
    // 测试配置默认值
    private static final int DEFAULT_NODE_COUNT = 3;
    private static final int DEFAULT_CONCURRENT_TASKS = 10;
    private static final int DEFAULT_MAX_TASK_COUNT = 1000;
    private static final int DEFAULT_TEST_TIMEOUT_MINUTES = 10;
    private static final int DEFAULT_TASK_TIMEOUT_MS = 30000;
    private static final int DEFAULT_TASK_PRIORITY = 5;
    private static final boolean DEFAULT_TASK_RETRYABLE = true;
    private static final int DEFAULT_MAX_RETRY_COUNT = 2;
    
    // 数据生成默认值
    private static final int DEFAULT_FILE_COUNT = 5;
    private static final int DEFAULT_RECORDS_PER_FILE = 200;
    private static final int DEFAULT_MIN_SIZE_KB = 1;
    private static final int DEFAULT_MAX_SIZE_KB = 10;
    private static final boolean DEFAULT_COMPRESS = true;
    
    /**
     * 测试主方法
     * 
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        try {
            // 解析命令行参数
            TestConfig config = parseArgs(args);
            
            // 输出测试配置
            printTestConfig(config);
            
            // 检查和创建测试数据
            checkAndCreateTestData(config);
            
            // 获取测试数据文件
            File dataDir = new File(config.testDataDir);
            File[] dataFiles = dataDir.listFiles((dir, name) -> 
                    name.startsWith("data_file_") && (name.endsWith(".json") || name.endsWith(".gz")));
            
            if (dataFiles == null || dataFiles.length == 0) {
                System.err.println("错误: 测试数据目录中没有找到数据文件");
                return;
            }
            
            // 保存单线程测试结果
            PerformanceTestClient.TestResult singleThreadResult = null;
            
            // 先运行单线程对比测试
            if (config.runSingleThreadTest) {
                System.out.println("\n============ 开始单线程对比测试 ============");
                singleThreadResult = runSingleThreadTest(dataFiles, config);
                generateTestReports(singleThreadResult, config, "single_thread_");
                printTestSummary(singleThreadResult, "单线程测试");
                System.out.println("============ 单线程对比测试完成 ============\n");
            }
            
            // 创建并初始化多节点模拟器
            MultiNodeSimulator simulator = new MultiNodeSimulator(config.nodeCount, config.baseDir + File.separator + "nodes");
            
            try {
                // 初始化模拟环境
                simulator.initialize();
                
                // 启动所有节点
                simulator.startAllNodes();
                
                if (simulator.getActiveNodeCount() == 0) {
                    System.err.println("错误: 没有成功启动的节点，测试中止");
                    return;
                }
                
                // 等待节点启动稳定
                System.out.println("等待节点稳定...");
                Thread.sleep(5000);
                
                // 创建性能测试客户端
                PerformanceTestClient testClient = new PerformanceTestClient(
                        config.testDataDir,
                        config.concurrentTasks,
                        config.taskTimeoutMs,
                        config.priority,
                        config.retryable,
                        config.maxRetryCount,
                        simulator.getAllFrameworks()
                );
                
                // 运行测试
                System.out.println("\n============ 开始并行处理测试 ============");
                PerformanceTestClient.TestResult result = testClient.runTest(config.maxTaskCount, config.testTimeoutMinutes);
                
                // 生成测试报告
                generateTestReports(result, config, "parallel_");
                
                // 输出测试结果摘要
                printTestSummary(result, "并行处理测试");
                System.out.println("============ 并行处理测试完成 ============\n");
                
                // 如果都运行了，显示对比结果
                if (config.runSingleThreadTest) {
                    compareTestResults(singleThreadResult, result);
                }
                
            } finally {
                // 停止模拟器并清理资源
                if (simulator.isRunning()) {
                    simulator.stopAllNodes();
                }
                
                if (config.cleanupAfterTest) {
                    simulator.cleanup();
                }
            }
            
        } catch (Exception e) {
            System.err.println("测试执行发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 解析命令行参数
     * 
     * @param args 命令行参数
     * @return 测试配置
     */
    private static TestConfig parseArgs(String[] args) {
        TestConfig config = new TestConfig();
        
        // 设置默认值
        config.nodeCount = DEFAULT_NODE_COUNT;
        config.concurrentTasks = DEFAULT_CONCURRENT_TASKS;
        config.maxTaskCount = DEFAULT_MAX_TASK_COUNT;
        config.testTimeoutMinutes = DEFAULT_TEST_TIMEOUT_MINUTES;
        config.taskTimeoutMs = DEFAULT_TASK_TIMEOUT_MS;
        config.priority = DEFAULT_TASK_PRIORITY;
        config.retryable = DEFAULT_TASK_RETRYABLE;
        config.maxRetryCount = DEFAULT_MAX_RETRY_COUNT;
        config.fileCount = DEFAULT_FILE_COUNT;
        config.recordsPerFile = DEFAULT_RECORDS_PER_FILE;
        config.minSizeKb = DEFAULT_MIN_SIZE_KB;
        config.maxSizeKb = DEFAULT_MAX_SIZE_KB;
        config.compress = DEFAULT_COMPRESS;
        config.generateData = false;
        config.cleanupAfterTest = true;
        config.runSingleThreadTest = true; // 默认进行单线程测试
        
        // 设置默认目录
        String userHome = "D:\\project\\pers\\parallel";
        config.baseDir = userHome + File.separator + "parallel-test";
        config.testDataDir = config.baseDir + File.separator + "test-data";
        config.reportDir = config.baseDir + File.separator + "reports";
        
        // 解析命令行参数
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            
            if (arg.equals("--nodes") && i + 1 < args.length) {
                config.nodeCount = Integer.parseInt(args[++i]);
            } else if (arg.equals("--concurrent") && i + 1 < args.length) {
                config.concurrentTasks = Integer.parseInt(args[++i]);
            } else if (arg.equals("--max-tasks") && i + 1 < args.length) {
                config.maxTaskCount = Integer.parseInt(args[++i]);
            } else if (arg.equals("--timeout") && i + 1 < args.length) {
                config.testTimeoutMinutes = Integer.parseInt(args[++i]);
            } else if (arg.equals("--task-timeout") && i + 1 < args.length) {
                config.taskTimeoutMs = Integer.parseInt(args[++i]);
            } else if (arg.equals("--priority") && i + 1 < args.length) {
                config.priority = Integer.parseInt(args[++i]);
            } else if (arg.equals("--retryable") && i + 1 < args.length) {
                config.retryable = Boolean.parseBoolean(args[++i]);
            } else if (arg.equals("--max-retry") && i + 1 < args.length) {
                config.maxRetryCount = Integer.parseInt(args[++i]);
            } else if (arg.equals("--base-dir") && i + 1 < args.length) {
                config.baseDir = args[++i];
                config.testDataDir = config.baseDir + File.separator + "test-data";
                config.reportDir = config.baseDir + File.separator + "reports";
            } else if (arg.equals("--data-dir") && i + 1 < args.length) {
                config.testDataDir = args[++i];
            } else if (arg.equals("--report-dir") && i + 1 < args.length) {
                config.reportDir = args[++i];
            } else if (arg.equals("--generate-data")) {
                config.generateData = true;
            } else if (arg.equals("--file-count") && i + 1 < args.length) {
                config.fileCount = Integer.parseInt(args[++i]);
            } else if (arg.equals("--records-per-file") && i + 1 < args.length) {
                config.recordsPerFile = Integer.parseInt(args[++i]);
            } else if (arg.equals("--min-size") && i + 1 < args.length) {
                config.minSizeKb = Integer.parseInt(args[++i]);
            } else if (arg.equals("--max-size") && i + 1 < args.length) {
                config.maxSizeKb = Integer.parseInt(args[++i]);
            } else if (arg.equals("--compress") && i + 1 < args.length) {
                config.compress = Boolean.parseBoolean(args[++i]);
            } else if (arg.equals("--keep-files")) {
                config.cleanupAfterTest = false;
            } else if (arg.equals("--no-single-thread")) {
                config.runSingleThreadTest = false;
            } else if (arg.equals("--help")) {
                printUsage();
                System.exit(0);
            }
        }
        
        return config;
    }
    
    /**
     * 打印使用帮助
     */
    private static void printUsage() {
        System.out.println("并行计算框架测试工具使用方法:");
        System.out.println("java -cp <classpath> pers.cclucky.parallel.test.ParallelFrameworkTest [选项]");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --nodes <数量>                 模拟节点数量 (默认: " + DEFAULT_NODE_COUNT + ")");
        System.out.println("  --concurrent <数量>            并发任务数 (默认: " + DEFAULT_CONCURRENT_TASKS + ")");
        System.out.println("  --max-tasks <数量>             最大任务数量 (默认: " + DEFAULT_MAX_TASK_COUNT + ")");
        System.out.println("  --timeout <分钟>               测试超时时间(分钟) (默认: " + DEFAULT_TEST_TIMEOUT_MINUTES + ")");
        System.out.println("  --task-timeout <毫秒>          任务超时时间(毫秒) (默认: " + DEFAULT_TASK_TIMEOUT_MS + ")");
        System.out.println("  --priority <优先级>            任务优先级 (默认: " + DEFAULT_TASK_PRIORITY + ")");
        System.out.println("  --retryable <true|false>       任务是否可重试 (默认: " + DEFAULT_TASK_RETRYABLE + ")");
        System.out.println("  --max-retry <次数>             最大重试次数 (默认: " + DEFAULT_MAX_RETRY_COUNT + ")");
        System.out.println("  --base-dir <路径>              测试基础目录 (默认: ~/parallel-test)");
        System.out.println("  --data-dir <路径>              测试数据目录 (默认: <base-dir>/test-data)");
        System.out.println("  --report-dir <路径>            测试报告目录 (默认: <base-dir>/reports)");
        System.out.println("  --generate-data                在测试前生成测试数据");
        System.out.println("  --file-count <数量>            生成的测试文件数量 (默认: " + DEFAULT_FILE_COUNT + ")");
        System.out.println("  --records-per-file <数量>      每个文件的记录数 (默认: " + DEFAULT_RECORDS_PER_FILE + ")");
        System.out.println("  --min-size <KB>                最小记录大小(KB) (默认: " + DEFAULT_MIN_SIZE_KB + ")");
        System.out.println("  --max-size <KB>                最大记录大小(KB) (默认: " + DEFAULT_MAX_SIZE_KB + ")");
        System.out.println("  --compress <true|false>        是否压缩测试数据 (默认: " + DEFAULT_COMPRESS + ")");
        System.out.println("  --keep-files                    测试后保留临时文件，不清理");
        System.out.println("  --no-single-thread              不进行单线程对比测试");
        System.out.println("  --help                          显示帮助信息");
    }
    
    /**
     * 打印测试配置
     * 
     * @param config 测试配置
     */
    private static void printTestConfig(TestConfig config) {
        System.out.println("========================================================");
        System.out.println("并行计算框架测试配置:");
        System.out.println("========================================================");
        System.out.println("节点数量: " + config.nodeCount);
        System.out.println("并发任务数: " + config.concurrentTasks);
        System.out.println("最大任务数量: " + config.maxTaskCount);
        System.out.println("测试超时时间: " + config.testTimeoutMinutes + " 分钟");
        System.out.println("任务超时时间: " + config.taskTimeoutMs + " 毫秒");
        System.out.println("任务优先级: " + config.priority);
        System.out.println("任务是否可重试: " + (config.retryable ? "是" : "否"));
        System.out.println("最大重试次数: " + config.maxRetryCount);
        System.out.println("测试基础目录: " + config.baseDir);
        System.out.println("测试数据目录: " + config.testDataDir);
        System.out.println("测试报告目录: " + config.reportDir);
        System.out.println("是否生成测试数据: " + (config.generateData ? "是" : "否"));
        if (config.generateData) {
            System.out.println("测试文件数量: " + config.fileCount);
            System.out.println("每个文件的记录数: " + config.recordsPerFile);
            System.out.println("记录大小范围: " + config.minSizeKb + " - " + config.maxSizeKb + " KB");
            System.out.println("是否压缩数据: " + (config.compress ? "是" : "否"));
        }
        System.out.println("测试后是否清理: " + (config.cleanupAfterTest ? "是" : "否"));
        System.out.println("是否进行单线程对比测试: " + (config.runSingleThreadTest ? "是" : "否"));
        System.out.println("========================================================");
        System.out.println();
    }
    
    /**
     * 检查并创建测试数据
     * 
     * @param config 测试配置
     */
    private static void checkAndCreateTestData(TestConfig config) throws Exception {
        File testDataDir = new File(config.testDataDir);
        
        // 检查测试数据目录是否存在
        if (!testDataDir.exists()) {
            if (config.generateData) {
                // 创建目录
                testDataDir.mkdirs();
                
                // 生成测试数据
                System.out.println("生成测试数据...");
                DataGenerator.generateTestData(
                        config.testDataDir,
                        config.fileCount,
                        config.recordsPerFile,
                        config.minSizeKb,
                        config.maxSizeKb,
                        config.compress
                );
            } else {
                throw new IllegalArgumentException("测试数据目录 " + config.testDataDir + " 不存在，请使用 --generate-data 选项或创建数据目录");
            }
        } else {
            if (config.generateData) {
                // 检查目录是否为空
                File[] files = testDataDir.listFiles();
                if (files != null && files.length > 0) {
                    System.out.println("测试数据目录已存在数据，将覆盖生成新数据...");
                    // 清空目录
                    for (File file : files) {
                        file.delete();
                    }
                }
                
                // 生成测试数据
                System.out.println("生成测试数据...");
                DataGenerator.generateTestData(
                        config.testDataDir,
                        config.fileCount,
                        config.recordsPerFile,
                        config.minSizeKb,
                        config.maxSizeKb,
                        config.compress
                );
            } else {
                // 检查是否有测试数据
                File[] files = testDataDir.listFiles((dir, name) -> name.startsWith("data_file_"));
                if (files == null || files.length == 0) {
                    throw new IllegalArgumentException("测试数据目录 " + config.testDataDir + " 不包含测试数据，请使用 --generate-data 选项生成数据");
                }
                
                // 估算总记录数
                long totalRecords = DataGenerator.countTotalRecords(config.testDataDir);
                System.out.println("发现 " + files.length + " 个测试数据文件，预计包含约 " + totalRecords + " 条记录");
            }
        }
        
        // 创建报告目录
        File reportDir = new File(config.reportDir);
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }
    }
    
    /**
     * 运行单线程对比测试
     * 
     * @param dataFiles 测试数据文件
     * @param config 测试配置
     * @return 测试结果
     * @throws Exception 如果测试执行过程中发生错误
     */
    private static PerformanceTestClient.TestResult runSingleThreadTest(File[] dataFiles, TestConfig config) throws Exception {
        System.out.println("运行单线程对比测试...");
        
        PerformanceTestClient.TestResult result = new PerformanceTestClient.TestResult();
        result.setStartTime(System.currentTimeMillis());
        
        int submittedTasks = 0;
        int completedTasks = 0;
        int failedTasks = 0;
        long totalProcessingTime = 0;
        List<Long> processingTimes = new ArrayList<>();
        
        int totalTaskCount = 0;
        int maxTaskCount = config.maxTaskCount;
        
        // 处理每个测试数据文件
        for (File dataFile : dataFiles) {
            System.out.println("处理文件: " + dataFile.getName());
            
            List<String> lines = readDataFile(dataFile);
            
            for (String line : lines) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                // 检查是否达到最大任务数
                if (maxTaskCount > 0 && totalTaskCount >= maxTaskCount) {
                    System.out.println("已达到指定的最大任务数: " + maxTaskCount);
                    break;
                }
                
                // 单线程直接处理任务
                submittedTasks++;
                totalTaskCount++;
                
                try {
                    // 记录任务开始时间
                    long taskStartTime = System.currentTimeMillis();
                    
                    // 执行处理 - 使用字符串反转作为处理逻辑
                    String resultStr = processDataSingleThread(line);
                    
                    // 记录任务完成时间
                    long taskEndTime = System.currentTimeMillis();
                    long processingTime = taskEndTime - taskStartTime;
                    
                    completedTasks++;
                    totalProcessingTime += processingTime;
                    processingTimes.add(processingTime);
                    
                    // 定期记录进度
                    if (submittedTasks % 100 == 0) {
                        System.out.println("进度: " + completedTasks + "/" + submittedTasks + " 完成, " + failedTasks + " 失败");
                    }
                } catch (Exception e) {
                    System.err.println("任务处理异常: " + e.getMessage());
                    failedTasks++;
                }
            }
            
            if (maxTaskCount > 0 && totalTaskCount >= maxTaskCount) {
                break;
            }
        }
        
        result.setEndTime(System.currentTimeMillis());
        result.setTotalTestTimeMs(result.getEndTime() - result.getStartTime());
        result.setSubmittedTasks(submittedTasks);
        result.setCompletedTasks(completedTasks);
        result.setFailedTasks(failedTasks);
        result.setCancelledTasks(0);
        
        if (!processingTimes.isEmpty()) {
            // 计算处理时间统计
            long avgProcessingTime = totalProcessingTime / processingTimes.size();
            
            // 计算中位数和百分位
            Collections.sort(processingTimes);
            
            long medianProcessingTime = processingTimes.get(processingTimes.size() / 2);
            long p95ProcessingTime = processingTimes.get((int)(processingTimes.size() * 0.95));
            long p99ProcessingTime = processingTimes.get((int)(processingTimes.size() * 0.99));
            long minProcessingTime = processingTimes.get(0);
            long maxProcessingTime = processingTimes.get(processingTimes.size() - 1);
            
            result.setMinProcessingTimeMs(minProcessingTime);
            result.setMaxProcessingTimeMs(maxProcessingTime);
            result.setAvgProcessingTimeMs(avgProcessingTime);
            result.setMedianProcessingTimeMs(medianProcessingTime);
            result.setP95ProcessingTimeMs(p95ProcessingTime);
            result.setP99ProcessingTimeMs(p99ProcessingTime);
        }
        
        System.out.println("单线程测试完成，耗时: " + (result.getTotalTestTimeMs() / 1000.0) + "秒");
        
        return result;
    }
    
    /**
     * 单线程处理数据
     * 
     * @param data 输入数据
     * @return 处理结果
     */
    private static String processDataSingleThread(String data) {
        // 高效的字符串反转
        StringBuilder sb = new StringBuilder(data);
        return sb.reverse().toString();
    }
    
    /**
     * 读取数据文件内容
     * 
     * @param dataFile 数据文件
     * @return 文件行列表
     * @throws IOException 如果读取过程中发生错误
     */
    private static List<String> readDataFile(File dataFile) throws IOException {
        List<String> lines = new ArrayList<>();
        
        if (dataFile.getName().endsWith(".gz")) {
            // 读取GZIP压缩文件
            try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(dataFile));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(gzis, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
        } else {
            // 读取普通文本文件
            try (BufferedReader reader = new BufferedReader(new FileReader(dataFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
        }
        
        return lines;
    }
    
    /**
     * 比较测试结果
     * 
     * @param singleThreadResult 单线程测试结果
     * @param parallelResult 并行测试结果
     */
    private static void compareTestResults(PerformanceTestClient.TestResult singleThreadResult, 
                                          PerformanceTestClient.TestResult parallelResult) {
        System.out.println("\n============ 测试结果对比 ============");
        System.out.println("指标\t\t单线程\t\t并行处理\t\t改进比例");
        
        double timeRatio = (double) singleThreadResult.getTotalTestTimeMs() / parallelResult.getTotalTestTimeMs();
        double avgTimeRatio = (double) singleThreadResult.getAvgProcessingTimeMs() / parallelResult.getAvgProcessingTimeMs();
        double throughputRatio = parallelResult.getThroughputPerSecond() / singleThreadResult.getThroughputPerSecond();
        
        System.out.printf("总耗时\t\t%.2f秒\t\t%.2f秒\t\t%.2f倍\n", 
                singleThreadResult.getTotalTestTimeMs() / 1000.0,
                parallelResult.getTotalTestTimeMs() / 1000.0,
                timeRatio);
        
        System.out.printf("平均处理时间\t%.2f毫秒\t\t%.2f毫秒\t\t%.2f倍\n", 
                (double) singleThreadResult.getAvgProcessingTimeMs(),
                (double) parallelResult.getAvgProcessingTimeMs(),
                avgTimeRatio);
        
        System.out.printf("吞吐量\t\t%.2f任务/秒\t%.2f任务/秒\t%.2f倍\n", 
                singleThreadResult.getThroughputPerSecond(),
                parallelResult.getThroughputPerSecond(),
                throughputRatio);
        
        System.out.printf("成功率\t\t%.2f%%\t\t%.2f%%\n", 
                singleThreadResult.getSuccessRate() * 100,
                parallelResult.getSuccessRate() * 100);
        
        System.out.println("============ 对比结束 ============");
    }
    
    /**
     * 打印测试结果摘要
     * 
     * @param result 测试结果
     * @param testType 测试类型名称
     */
    private static void printTestSummary(PerformanceTestClient.TestResult result, String testType) {
        System.out.println("\n" + testType + "结果摘要:");
        System.out.println("总测试时间: " + formatTime(result.getTotalTestTimeMs()));
        System.out.println("提交任务数: " + result.getSubmittedTasks());
        System.out.println("成功完成数: " + result.getCompletedTasks() + " (" + String.format("%.2f%%", result.getSuccessRate() * 100) + ")");
        System.out.println("失败任务数: " + result.getFailedTasks());
        System.out.println("取消任务数: " + result.getCancelledTasks());
        System.out.println("平均处理时间: " + result.getAvgProcessingTimeMs() + " ms");
        System.out.println("中位数处理时间: " + result.getMedianProcessingTimeMs() + " ms");
        System.out.println("95%分位处理时间: " + result.getP95ProcessingTimeMs() + " ms");
        System.out.println("吞吐量: " + String.format("%.2f", result.getThroughputPerSecond()) + " 任务/秒");
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
     * 生成测试报告
     * 
     * @param result 测试结果
     * @param config 测试配置
     * @param prefix 文件名前缀
     * @throws Exception 如果报告生成过程中发生错误
     */
    private static void generateTestReports(PerformanceTestClient.TestResult result, TestConfig config, String prefix) throws Exception {
        // 创建报告目录
        File reportDir = new File(config.reportDir);
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }
        
        // 生成报告文件名
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        String reportFileName = prefix + "test_report_" + timestamp + ".html";
        
        // 创建测试报告配置
        TestReportGenerator.TestConfig reportConfig = new TestReportGenerator.TestConfig();
        reportConfig.setNodeCount(config.nodeCount);
        reportConfig.setConcurrentTasks(config.concurrentTasks);
        reportConfig.setTestDataDir(config.testDataDir);
        reportConfig.setPriority(config.priority);
        reportConfig.setTaskTimeoutMs(config.taskTimeoutMs);
        reportConfig.setRetryable(config.retryable);
        reportConfig.setMaxRetryCount(config.maxRetryCount);
        
        // 生成HTML报告
        String htmlReportPath = config.reportDir + File.separator + reportFileName;
        System.out.println("生成HTML测试报告: " + htmlReportPath);
        TestReportGenerator.generateHtmlReport(result, htmlReportPath, reportConfig);
        
        // 生成文本报告
        String textReportPath = config.reportDir + File.separator + prefix + "test_report_" + timestamp + ".txt";
        System.out.println("生成文本测试报告: " + textReportPath);
        TestReportGenerator.generateTextReport(result, textReportPath, reportConfig);
    }
    
    /**
     * 测试配置
     */
    private static class TestConfig {
        private int nodeCount;
        private int concurrentTasks;
        private int maxTaskCount;
        private int testTimeoutMinutes;
        private long taskTimeoutMs;
        private int priority;
        private boolean retryable;
        private int maxRetryCount;
        
        private String baseDir;
        private String testDataDir;
        private String reportDir;
        
        private boolean generateData;
        private int fileCount;
        private int recordsPerFile;
        private int minSizeKb;
        private int maxSizeKb;
        private boolean compress;
        
        private boolean cleanupAfterTest;
        private boolean runSingleThreadTest;
    }
} 