package pers.cclucky.test;

import pers.cclucky.parallel.ParallelFramework;
import pers.cclucky.parallel.api.service.TaskService;
import pers.cclucky.parallel.client.ParallelClient;

import java.util.List;

/**
 * 性能测试启动器
 * 提供命令行界面启动测试
 */
public class TestLauncher {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestLauncher.class);
    
    // 测试模式
    private enum TestMode {
        PARALLEL_ONLY,    // 仅测试并行框架
        SINGLE_ONLY,      // 仅测试单线程
        COMPARISON        // 对比测试
    }
    
    // 默认配置值
    private static final String DEFAULT_DATA_DIR = "test-data";
    private static final String DEFAULT_REPORT_DIR = "test-reports";
    private static final int DEFAULT_CONCURRENT_TASKS = 10;
    private static final int DEFAULT_MAX_TASKS = 1000;
    private static final int DEFAULT_TIMEOUT_MINUTES = 10;
    private static final int DEFAULT_FILE_COUNT = 2;
    private static final int DEFAULT_STRINGS_PER_FILE = 100;
    private static final int DEFAULT_MIN_STRING_LENGTH = 10;
    private static final int DEFAULT_MAX_STRING_LENGTH = 200;
    private static final boolean DEFAULT_GENERATE_DATA = true;
    private static final boolean DEFAULT_VERIFY_RESULTS = true;
    private static final boolean DEFAULT_GENERATE_REPORT = true;
    
    public static void main(String[] args) {
        try {
            // 解析命令行参数
            ParallelPerformanceTest.TestConfig config = parseArgs(args);
            TestMode testMode = parseTestMode(args);
            
            // 输出测试配置
            printTestConfig(config, testMode);
            
            // 准备测试数据
            List<String> testStrings = null;

            // 执行单线程测试
            ParallelPerformanceTest.TestResult singleThreadResult = null;

            if (testMode == TestMode.SINGLE_ONLY || testMode == TestMode.COMPARISON) {
                // 如果是单线程测试模式且没有测试数据，需要生成测试数据
                if (testStrings == null) {
                    if (config.isGenerateData()) {
                        logger.info("生成测试数据...");
                        StringDataGenerator.generateStringDataFiles(
                                config.getDataDir(),
                                config.getFileCount(),
                                config.getStringsPerFile(),
                                config.getMinStringLength(),
                                config.getMaxStringLength()
                        );
                    }
                    testStrings = StringDataGenerator.loadTestStrings(config.getDataDir(), config.getMaxTasks());
                }

                logger.info("开始执行单线程性能测试...");

                // 创建单线程处理器
                SingleThreadProcessor singleProcessor = new SingleThreadProcessor();

                // 执行单线程测试
                singleThreadResult = singleProcessor.runTest(testStrings, config);

                // 输出单线程结果摘要
                if (testMode == TestMode.SINGLE_ONLY) {
                    printTestSummary(singleThreadResult, "单线程测试");
                }
            }
            
            // 如果需要进行并行框架测试或对比测试，初始化并行框架
            ParallelFramework framework = null;
            ParallelClient client = null;
            ParallelPerformanceTest.TestResult parallelResult = null;
            
            if (testMode == TestMode.PARALLEL_ONLY || testMode == TestMode.COMPARISON) {
                logger.info("初始化并行框架...");
                
                // 初始化并行框架
                framework = new ParallelFramework("application.properties");
                framework.init();
                framework.start();
                
                // 获取TaskService
                TaskService taskService = framework.getService("taskService");
                if (taskService == null) {
                    logger.error("无法获取TaskService服务，请检查框架配置");
                    return;
                }
                
                // 创建客户端
                client = new ParallelClient(taskService);
                
                // 创建性能测试器
                ParallelPerformanceTest tester = new ParallelPerformanceTest(client, config);
                
                logger.info("开始执行并行框架性能测试...");
                
                // 执行测试
                parallelResult = tester.runTest();
                
                // 获取测试数据以便用于单线程测试
                if (testMode == TestMode.COMPARISON) {
                    testStrings = StringDataGenerator.loadTestStrings(config.getDataDir(), config.getMaxTasks());
                }
                
                // 输出并行框架结果摘要
                if (testMode == TestMode.PARALLEL_ONLY) {
                    printTestSummary(parallelResult, "并行框架测试");
                }
            }
            
            // 如果是对比测试模式，生成对比报告
            if (testMode == TestMode.COMPARISON && parallelResult != null && singleThreadResult != null) {
                logger.info("生成对比测试报告...");
                
                // 生成对比报告
                ComparisonReportGenerator.generateHtmlReport(
                        parallelResult,
                        singleThreadResult,
                        config,
                        config.getReportDir()
                );
                
                // 输出对比结果摘要
                printComparisonSummary(parallelResult, singleThreadResult);
            }
            
            logger.info("测试完成，退出框架...");
            
            // 关闭框架
//            if (framework != null) {
//                framework.stop();
//            }
            
        } catch (Exception e) {
            logger.error("测试执行过程中发生错误", e);
        }
    }
    
    /**
     * 解析测试模式
     * 
     * @param args 命令行参数
     * @return 测试模式
     */
    private static TestMode parseTestMode(String[] args) {
        for (String arg : args) {
            if ("--single-only".equals(arg)) {
                return TestMode.SINGLE_ONLY;
            } else if ("--parallel-only".equals(arg)) {
                return TestMode.PARALLEL_ONLY;
            } else if ("--comparison".equals(arg)) {
                return TestMode.COMPARISON;
            }
        }
        
        // 默认为对比测试
        return TestMode.COMPARISON;
    }
    
    /**
     * 创建默认测试配置
     * 
     * @return 默认配置对象
     */
    private static ParallelPerformanceTest.TestConfig createDefaultConfig() {
        ParallelPerformanceTest.TestConfig config = new ParallelPerformanceTest.TestConfig();
        
        // 设置测试数据相关配置
        config.setDataDir(DEFAULT_DATA_DIR);
        config.setReportDir(DEFAULT_REPORT_DIR);
        config.setMaxTasks(DEFAULT_MAX_TASKS);
        config.setTimeoutMinutes(DEFAULT_TIMEOUT_MINUTES);
        config.setGenerateData(DEFAULT_GENERATE_DATA);
        config.setFileCount(DEFAULT_FILE_COUNT);
        config.setStringsPerFile(DEFAULT_STRINGS_PER_FILE);
        config.setMinStringLength(DEFAULT_MIN_STRING_LENGTH);
        config.setMaxStringLength(DEFAULT_MAX_STRING_LENGTH);
        
        // 设置并行框架相关配置
        config.setConcurrentTasks(DEFAULT_CONCURRENT_TASKS);
        config.setVerifyResults(DEFAULT_VERIFY_RESULTS);
        
        // 设置报告相关配置
        config.setGenerateReport(DEFAULT_GENERATE_REPORT);
        
        return config;
    }
    
    /**
     * 解析命令行参数
     * 
     * @param args 命令行参数
     * @return 测试配置
     */
    private static ParallelPerformanceTest.TestConfig parseArgs(String[] args) {
        // 首先创建默认配置
        ParallelPerformanceTest.TestConfig config = createDefaultConfig();
        
        // 然后根据命令行参数修改默认配置
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            
            if ("--data-dir".equals(arg) && i + 1 < args.length) {
                config.setDataDir(args[++i]);
            } else if ("--report-dir".equals(arg) && i + 1 < args.length) {
                config.setReportDir(args[++i]);
            } else if ("--concurrent".equals(arg) && i + 1 < args.length) {
                config.setConcurrentTasks(Integer.parseInt(args[++i]));
            } else if ("--max-tasks".equals(arg) && i + 1 < args.length) {
                config.setMaxTasks(Integer.parseInt(args[++i]));
            } else if ("--timeout".equals(arg) && i + 1 < args.length) {
                config.setTimeoutMinutes(Integer.parseInt(args[++i]));
            } else if ("--generate-data".equals(arg)) {
                config.setGenerateData(true);
            } else if ("--no-generate-data".equals(arg)) {
                config.setGenerateData(false);
            } else if ("--file-count".equals(arg) && i + 1 < args.length) {
                config.setFileCount(Integer.parseInt(args[++i]));
            } else if ("--strings-per-file".equals(arg) && i + 1 < args.length) {
                config.setStringsPerFile(Integer.parseInt(args[++i]));
            } else if ("--min-length".equals(arg) && i + 1 < args.length) {
                config.setMinStringLength(Integer.parseInt(args[++i]));
            } else if ("--max-length".equals(arg) && i + 1 < args.length) {
                config.setMaxStringLength(Integer.parseInt(args[++i]));
            } else if ("--verify".equals(arg)) {
                config.setVerifyResults(true);
            } else if ("--no-verify".equals(arg)) {
                config.setVerifyResults(false);
            } else if ("--report".equals(arg)) {
                config.setGenerateReport(true);
            } else if ("--no-report".equals(arg)) {
                config.setGenerateReport(false);
            } else if ("--help".equals(arg)) {
                printUsage();
                System.exit(0);
            }
        }
        
        return config;
    }
    
    /**
     * 输出使用帮助
     */
    private static void printUsage() {
        System.out.println("并行计算框架字符串反转性能测试工具");
        System.out.println("用法: java -cp <classpath> pers.cclucky.test.TestLauncher [选项]");
        System.out.println();
        System.out.println("测试模式选项:");
        System.out.println("  --comparison              执行对比测试 (默认)");
        System.out.println("  --parallel-only           仅测试并行框架");
        System.out.println("  --single-only             仅测试单线程处理");
        System.out.println();
        System.out.println("通用选项:");
        System.out.println("  --data-dir <目录>          测试数据目录 (默认: " + DEFAULT_DATA_DIR + ")");
        System.out.println("  --report-dir <目录>        测试报告目录 (默认: " + DEFAULT_REPORT_DIR + ")");
        System.out.println("  --max-tasks <数量>         最大任务数量 (默认: " + DEFAULT_MAX_TASKS + ")");
        System.out.println("  --timeout <分钟>           测试超时时间(分钟) (默认: " + DEFAULT_TIMEOUT_MINUTES + ")");
        System.out.println("  --generate-data            生成测试数据 (默认)");
        System.out.println("  --no-generate-data         不生成测试数据，使用已有数据");
        System.out.println("  --file-count <数量>        生成的测试文件数量 (默认: " + DEFAULT_FILE_COUNT + ")");
        System.out.println("  --strings-per-file <数量>  每个文件的字符串数量 (默认: " + DEFAULT_STRINGS_PER_FILE + ")");
        System.out.println("  --min-length <长度>        最小字符串长度 (默认: " + DEFAULT_MIN_STRING_LENGTH + ")");
        System.out.println("  --max-length <长度>        最大字符串长度 (默认: " + DEFAULT_MAX_STRING_LENGTH + ")");
        System.out.println();
        System.out.println("并行框架选项:");
        System.out.println("  --concurrent <数量>        并发任务数 (默认: " + DEFAULT_CONCURRENT_TASKS + ")");
        System.out.println("  --verify                   验证任务结果 (默认)");
        System.out.println("  --no-verify                不验证任务结果");
        System.out.println();
        System.out.println("报告选项:");
        System.out.println("  --report                   生成测试报告 (默认)");
        System.out.println("  --no-report                不生成测试报告");
        System.out.println("  --help                     显示此帮助信息");
    }
    
    /**
     * 输出测试配置
     * 
     * @param config 测试配置
     * @param testMode 测试模式
     */
    private static void printTestConfig(ParallelPerformanceTest.TestConfig config, TestMode testMode) {
        System.out.println("========================================================");
        System.out.println("并行计算框架字符串反转性能测试配置");
        System.out.println("========================================================");
        System.out.println("测试模式: " + testMode);
        System.out.println("测试数据目录: " + config.getDataDir());
        System.out.println("测试报告目录: " + config.getReportDir());
        
        if (testMode == TestMode.PARALLEL_ONLY || testMode == TestMode.COMPARISON) {
            System.out.println("并发任务数: " + config.getConcurrentTasks());
        }
        
        System.out.println("最大任务数量: " + config.getMaxTasks());
        System.out.println("测试超时时间: " + config.getTimeoutMinutes() + "分钟");
        System.out.println("是否生成测试数据: " + (config.isGenerateData() ? "是" : "否"));
        
        if (config.isGenerateData()) {
            System.out.println("生成文件数量: " + config.getFileCount());
            System.out.println("每个文件字符串数量: " + config.getStringsPerFile());
            System.out.println("字符串长度范围: " + config.getMinStringLength() + " - " + config.getMaxStringLength() + " 字符");
        }
        
        if (testMode == TestMode.PARALLEL_ONLY || testMode == TestMode.COMPARISON) {
            System.out.println("是否验证结果: " + (config.isVerifyResults() ? "是" : "否"));
        }
        
        System.out.println("是否生成报告: " + (config.isGenerateReport() ? "是" : "否"));
        System.out.println("========================================================");
        System.out.println();
    }
    
    /**
     * 输出测试结果摘要
     * 
     * @param result 测试结果
     * @param title 标题
     */
    private static void printTestSummary(ParallelPerformanceTest.TestResult result, String title) {
        System.out.println();
        System.out.println("========================================================");
        System.out.println(title + " 结果摘要");
        System.out.println("========================================================");
        System.out.println("测试持续时间: " + formatTime(result.getTotalTestTimeMs()));
        System.out.println("提交任务数: " + result.getTotalTasks());
        System.out.println("完成任务数: " + result.getCompletedTasks() + 
                " (" + String.format("%.2f%%", (result.getCompletedTasks() * 100.0 / result.getTotalTasks())) + ")");
        System.out.println("失败任务数: " + result.getFailedTasks() + 
                " (" + String.format("%.2f%%", (result.getFailedTasks() * 100.0 / result.getTotalTasks())) + ")");
        System.out.println("吞吐量: " + String.format("%.2f", result.getThroughputPerSecond()) + " 任务/秒");
        
        if (result.getCompletedTasks() > 0) {
            System.out.println("平均处理时间: " + formatTime(result.getAvgProcessingTimeMs()));
            System.out.println("中位数处理时间: " + formatTime(result.getMedianProcessingTimeMs()));
            System.out.println("95%分位处理时间: " + formatTime(result.getP95ProcessingTimeMs()));
            System.out.println("最小处理时间: " + formatTime(result.getMinProcessingTimeMs()));
            System.out.println("最大处理时间: " + formatTime(result.getMaxProcessingTimeMs()));
        }
        
        System.out.println("========================================================");
    }
    
    /**
     * 输出对比测试结果摘要
     * 
     * @param parallelResult 并行框架测试结果
     * @param singleThreadResult 单线程测试结果
     */
    private static void printComparisonSummary(
            ParallelPerformanceTest.TestResult parallelResult,
            ParallelPerformanceTest.TestResult singleThreadResult) {
        System.out.println();
        System.out.println("========================================================");
        System.out.println("性能对比测试结果摘要");
        System.out.println("========================================================");
        
        // 吞吐量对比
        double parallelThroughput = parallelResult.getThroughputPerSecond();
        double singleThroughput = singleThreadResult.getThroughputPerSecond();
        double throughputImprovement = ((parallelThroughput / singleThroughput) - 1) * 100;
        
        System.out.println("吞吐量 (任务/秒):");
        System.out.println("  并行框架: " + String.format("%.2f", parallelThroughput));
        System.out.println("  单线程: " + String.format("%.2f", singleThroughput));
        if (throughputImprovement > 0) {
            System.out.println("  提升比例: +" + String.format("%.2f%%", throughputImprovement));
        } else {
            System.out.println("  提升比例: " + String.format("%.2f%%", throughputImprovement));
        }
        
        // 总处理时间对比
        long parallelTotalTime = parallelResult.getTotalTestTimeMs();
        long singleTotalTime = singleThreadResult.getTotalTestTimeMs();
        double totalTimeImprovement = ((double)(singleTotalTime - parallelTotalTime) / singleTotalTime) * 100;
        
        System.out.println("总处理时间:");
        System.out.println("  并行框架: " + formatTime(parallelTotalTime));
        System.out.println("  单线程: " + formatTime(singleTotalTime));
        if (totalTimeImprovement > 0) {
            System.out.println("  节省比例: +" + String.format("%.2f%%", totalTimeImprovement));
        } else {
            System.out.println("  节省比例: " + String.format("%.2f%%", totalTimeImprovement));
        }
        
        System.out.println();
        System.out.println("詳細对比报告已生成，请查看: " + new java.io.File("test-reports").getAbsolutePath());
        System.out.println("========================================================");
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
} 