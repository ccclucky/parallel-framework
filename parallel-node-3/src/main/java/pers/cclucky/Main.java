package pers.cclucky;

import pers.cclucky.parallel.ParallelFramework;
import pers.cclucky.parallel.api.TaskResult;
import pers.cclucky.parallel.api.service.TaskService;
import pers.cclucky.parallel.client.ParallelClient;
import pers.cclucky.test.ParallelPerformanceTest;
import pers.cclucky.test.SingleThreadProcessor;
import pers.cclucky.test.TestLauncher;
import pers.cclucky.test.ComparisonReportGenerator;

import java.util.Scanner;
import java.util.List;
import java.util.ArrayList;

public class Main {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            if (args.length > 0) {
                if ("--test".equals(args[0])) {
                    // 测试模式：如果第一个参数是--test，则启动性能测试
                    logger.info("启动性能测试模式");
                    
                    // 准备传递给TestLauncher的参数
                    String[] testArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, testArgs, 0, args.length - 1);
                    
                    // 启动测试
                    TestLauncher.main(testArgs);
                    return;
                } else if ("--compare".equals(args[0])) {
                    // 对比测试模式
                    logger.info("启动对比测试模式");
                    
                    // 创建默认配置，并传递给TestLauncher
                    List<String> testArgsList = new ArrayList<>();
                    testArgsList.add("--comparison");
                    
                    // 添加其他参数
                    for (int i = 1; i < args.length; i++) {
                        testArgsList.add(args[i]);
                    }
                    
                    // 转换为数组并启动测试
                    String[] testArgs = testArgsList.toArray(new String[0]);
                    TestLauncher.main(testArgs);
                    return;
                }
            }

            // 交互模式：启动命令行交互界面
            logger.info("启动命令行交互模式");
            
            // 初始化并行框架
            ParallelFramework framework = new ParallelFramework("application.properties");
            framework.init();
            framework.start();

            // 获取TaskService
            TaskService taskService = framework.getService("taskService");

            // 使用ParallelClient
            ParallelClient client = new ParallelClient(taskService);

            // 启动交互线程
            Thread interactiveThread = new Thread(() -> runInteractiveMode(client));
            interactiveThread.start();

            // 等待交互线程结束
            interactiveThread.join();

            // 停止框架
            framework.stop();
            
        } catch (Exception e) {
            logger.error("应用程序执行错误", e);
        }
    }
    
    /**
     * 运行命令行交互模式
     * 
     * @param client 并行客户端
     */
    private static void runInteractiveMode(ParallelClient client) {
        // 创建Scanner对象读取用户输入
        Scanner scanner = new Scanner(System.in);

        System.out.println("==== 字符串反转服务 ====");
        System.out.println("输入要反转的字符串，或输入'exit'退出");
        System.out.println("输入'test'启动简单性能测试");
        System.out.println("输入'compare'启动对比测试");

        String input;
        while (true) {
            System.out.print("> ");
            input = scanner.nextLine().trim();

            // 检查是否退出
            if ("exit".equalsIgnoreCase(input)) {
                System.out.println("正在退出...");
                break;
            }
            
            // 检查是否启动测试
            if ("test".equalsIgnoreCase(input)) {
                System.out.println("启动简单性能测试...");
                try {
                    runSimpleTest(client);
                } catch (Exception e) {
                    System.out.println("测试执行失败: " + e.getMessage());
                }
                continue;
            }
            
            // 检查是否启动对比测试
            if ("compare".equalsIgnoreCase(input)) {
                System.out.println("启动对比性能测试...");
                try {
                    runComparisonTest(client);
                } catch (Exception e) {
                    System.out.println("对比测试执行失败: " + e.getMessage());
                }
                continue;
            }

            // 如果输入为空，提示并继续
            if (input.isEmpty()) {
                System.out.println("请输入非空字符串");
                continue;
            }

            try {
                // 创建任务
                StringReverseTask task = new StringReverseTask(input);

                // 提交任务并等待结果
                TaskResult<String> result = client.submitAndWait(task);

                // 输出结果
                if (result.isSuccess()) {
                    System.out.println("反转结果: " + result.getResult());
                } else {
                    System.out.println("任务执行失败: " + result.getErrorString());
                }
            } catch (Exception e) {
                System.out.println("处理失败: " + e.getMessage());
            }
        }

        // 关闭Scanner
        scanner.close();
    }
    
    /**
     * 运行简单的性能测试
     * 
     * @param client 并行客户端
     * @throws Exception 如果测试执行过程中发生错误
     */
    private static void runSimpleTest(ParallelClient client) throws Exception {
        // 创建默认测试配置，生成较少的测试数据
        ParallelPerformanceTest.TestConfig config = new ParallelPerformanceTest.TestConfig();
        config.setFileCount(2);
        config.setStringsPerFile(50);
        config.setMaxTasks(100);
        config.setConcurrentTasks(5);
        config.setTimeoutMinutes(5);
        
        System.out.println("运行快速性能测试，配置:");
        System.out.println("- 测试字符串: 100个");
        System.out.println("- 并发任务数: 5");
        System.out.println("- 超时时间: 5分钟");
        
        // 创建测试器
        ParallelPerformanceTest tester = new ParallelPerformanceTest(client, config);
        
        // 运行测试
        System.out.println("测试开始...");
        ParallelPerformanceTest.TestResult result = tester.runTest();
        
        // 输出结果
        System.out.println("测试完成!");
        System.out.println("总任务数: " + result.getTotalTasks());
        System.out.println("完成任务数: " + result.getCompletedTasks());
        System.out.println("吞吐量: " + String.format("%.2f", result.getThroughputPerSecond()) + " 任务/秒");
        System.out.println("平均处理时间: " + formatTime(result.getAvgProcessingTimeMs()));
        System.out.println("测试报告已保存到: " + config.getReportDir() + " 目录");
    }
    
    /**
     * 运行对比性能测试
     * 
     * @param client 并行客户端
     * @throws Exception 如果测试执行过程中发生错误
     */
    private static void runComparisonTest(ParallelClient client) throws Exception {
        // 创建默认测试配置，生成较少的测试数据
        ParallelPerformanceTest.TestConfig config = new ParallelPerformanceTest.TestConfig();
        config.setFileCount(2);
        config.setStringsPerFile(50);
        config.setMaxTasks(100);
        config.setConcurrentTasks(5);
        config.setTimeoutMinutes(5);
        
        System.out.println("运行并行与单线程对比测试，配置:");
        System.out.println("- 测试字符串: 100个");
        System.out.println("- 并发任务数: 5");
        System.out.println("- 超时时间: 5分钟");
        
        // 运行并行框架测试
        System.out.println("\n第1步: 执行并行框架测试...");
        ParallelPerformanceTest tester = new ParallelPerformanceTest(client, config);
        ParallelPerformanceTest.TestResult parallelResult = tester.runTest();
        
        // 获取测试数据
        System.out.println("\n第2步: 加载测试数据...");
        List<String> testStrings = new ArrayList<>(parallelResult.getTotalTasks());
        
        // 使用pers.cclucky.test.StringDataGenerator加载测试数据
        testStrings = pers.cclucky.test.StringDataGenerator.loadTestStrings(config.getDataDir(), config.getMaxTasks());
        
        System.out.println("加载了 " + testStrings.size() + " 个测试字符串");
        
        // 运行单线程测试
        System.out.println("\n第3步: 执行单线程测试...");
        SingleThreadProcessor singleProcessor = new SingleThreadProcessor();
        ParallelPerformanceTest.TestResult singleThreadResult = singleProcessor.runTest(testStrings, config);
        
        // 生成对比报告
        System.out.println("\n第4步: 生成对比报告...");
        ComparisonReportGenerator.generateHtmlReport(
                parallelResult, 
                singleThreadResult,
                config,
                config.getReportDir()
        );
        
        // 输出对比结果
        System.out.println("\n对比测试完成!");
        
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
        
        System.out.println("\n详细对比报告已保存到: " + config.getReportDir() + " 目录");
    }
    
    /**
     * 格式化时间
     */
    private static String formatTime(long timeMs) {
        if (timeMs < 1000) {
            return timeMs + " 毫秒";
        } else if (timeMs < 60000) {
            return String.format("%.2f 秒", timeMs / 1000.0);
        } else {
            return String.format("%.2f 分钟", timeMs / 60000.0);
        }
    }
}