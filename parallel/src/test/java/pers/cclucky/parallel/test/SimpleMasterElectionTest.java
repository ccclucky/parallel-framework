package pers.cclucky.parallel.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 简化版Master选举测试
 * 使用SimpleMultiNodeSimulator代替完整的并行计算框架，
 * 仅测试Master选举功能
 */
public class SimpleMasterElectionTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleMasterElectionTest.class);
    
    // 测试配置默认值
    private static final int DEFAULT_NODE_COUNT = 3;
    private static final int DEFAULT_TEST_DURATION_SECONDS = 60;
    
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
            
            // 创建测试报告目录
            createReportDirectory(config);
            
            // 创建并初始化多节点模拟器
            SimpleMultiNodeSimulator simulator = new SimpleMultiNodeSimulator(
                    config.nodeCount, 
                    config.baseDir + File.separator + "nodes"
            );
            
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
                
                // 输出初始Master信息
                String initialMasterId = simulator.getCurrentMasterId();
                System.out.println("\n============ Master选举测试开始 ============");
                System.out.println("初始Master节点: " + initialMasterId);
                System.out.println("活跃节点数量: " + simulator.getActiveNodeCount());
                
                // 开始测试计时
                long startTime = System.currentTimeMillis();
                long endTime = startTime + (config.testDurationSeconds * 1000);
                
                // 创建测试报告生成器
                SimpleTestReportGenerator reportGenerator = new SimpleTestReportGenerator(
                        config.reportDir + File.separator + "master_election_report.md"
                );
                
                // 初始化报告
                reportGenerator.beginReport("Master选举测试报告");
                reportGenerator.addSection("测试配置");
                reportGenerator.addKeyValue("测试时间", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                reportGenerator.addKeyValue("节点数量", String.valueOf(config.nodeCount));
                reportGenerator.addKeyValue("测试持续时间", config.testDurationSeconds + " 秒");
                reportGenerator.addKeyValue("初始Master节点", initialMasterId);
                
                reportGenerator.addSection("Master选举事件记录");
                reportGenerator.addTableHeader("时间", "事件", "详情");
                
                String currentMasterId = initialMasterId;
                
                // 主测试循环
                while (System.currentTimeMillis() < endTime) {
                    // 检查Master是否发生变化
                    String newMasterId = simulator.getCurrentMasterId();
                    if (newMasterId != null && !newMasterId.equals(currentMasterId)) {
                        String event = "Master节点变更";
                        String detail = currentMasterId + " -> " + newMasterId;
                        
                        System.out.println("[" + formatTime(System.currentTimeMillis() - startTime) + "] " + 
                                event + ": " + detail);
                        
                        reportGenerator.addTableRow(
                                formatTime(System.currentTimeMillis() - startTime),
                                event,
                                detail
                        );
                        
                        currentMasterId = newMasterId;
                    }
                    
                    // 每5秒模拟一次随机节点下线或上线
                    if (System.currentTimeMillis() - startTime > 15000 && // 等待15秒稳定期
                            config.simulateNodeFailure && 
                            System.currentTimeMillis() % 5000 < 100) { // 每5秒左右触发一次
                        
                        boolean shouldStopNode = Math.random() < 0.5; // 随机决定是停止还是启动节点
                        
                        if (shouldStopNode && simulator.getActiveNodeCount() > 1) {
                            // 随机停止一个节点
                            int nodeIndex = (int) (Math.random() * simulator.getAllNodes().size());
                            try {
                                String nodeId = simulator.getNode(nodeIndex).getNodeId();
                                
                                // 不要停止当前的Master节点
                                if (!nodeId.equals(simulator.getCurrentMasterId())) {
                                    System.out.println("[" + formatTime(System.currentTimeMillis() - startTime) + "] " + 
                                            "模拟节点下线: " + nodeId);
                                    
                                    simulator.getNode(nodeIndex).stop();
                                    
                                    reportGenerator.addTableRow(
                                            formatTime(System.currentTimeMillis() - startTime),
                                            "节点下线",
                                            nodeId
                                    );
                                    
                                    // 等待事件传播
                                    Thread.sleep(2000);
                                }
                            } catch (Exception e) {
                                logger.error("模拟节点下线失败", e);
                            }
                        }
                    }
                    
                    // 暂停一段时间
                    Thread.sleep(100);
                }
                
                // 测试结束
                System.out.println("\n测试完成，当前Master节点: " + simulator.getCurrentMasterId());
                
                // 完成报告
                reportGenerator.addSection("测试结果");
                reportGenerator.addKeyValue("最终Master节点", simulator.getCurrentMasterId());
                reportGenerator.addKeyValue("活跃节点数量", String.valueOf(simulator.getActiveNodeCount()));
                reportGenerator.renderTable(); // 确保渲染表格
                reportGenerator.finishReport();
                
                System.out.println("测试报告已生成: " + reportGenerator.getReportPath());
                System.out.println("============ Master选举测试结束 ============\n");
                
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
        config.testDurationSeconds = DEFAULT_TEST_DURATION_SECONDS;
        config.cleanupAfterTest = true;
        config.simulateNodeFailure = true;
        
        // 设置默认目录
        String userHome = System.getProperty("user.home");
        config.baseDir = userHome + File.separator + "parallel-test";
        config.reportDir = config.baseDir + File.separator + "reports";
        
        // 解析命令行参数
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            
            if (arg.equals("--nodes") && i + 1 < args.length) {
                config.nodeCount = Integer.parseInt(args[++i]);
            } else if (arg.equals("--duration") && i + 1 < args.length) {
                config.testDurationSeconds = Integer.parseInt(args[++i]);
            } else if (arg.equals("--base-dir") && i + 1 < args.length) {
                config.baseDir = args[++i];
                config.reportDir = config.baseDir + File.separator + "reports";
            } else if (arg.equals("--report-dir") && i + 1 < args.length) {
                config.reportDir = args[++i];
            } else if (arg.equals("--keep-files")) {
                config.cleanupAfterTest = false;
            } else if (arg.equals("--no-failures")) {
                config.simulateNodeFailure = false;
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
        System.out.println("Master选举测试工具使用方法:");
        System.out.println("java -cp <classpath> pers.cclucky.parallel.test.SimpleMasterElectionTest [选项]");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --nodes <数量>           模拟节点数量 (默认: " + DEFAULT_NODE_COUNT + ")");
        System.out.println("  --duration <秒>          测试持续时间(秒) (默认: " + DEFAULT_TEST_DURATION_SECONDS + ")");
        System.out.println("  --base-dir <目录>        基础目录路径");
        System.out.println("  --report-dir <目录>      测试报告目录路径");
        System.out.println("  --keep-files             测试后保留临时文件");
        System.out.println("  --no-failures            不模拟节点故障");
        System.out.println("  --help                   显示此帮助");
    }
    
    /**
     * 打印测试配置
     * 
     * @param config 测试配置
     */
    private static void printTestConfig(TestConfig config) {
        System.out.println("========== 测试配置 ==========");
        System.out.println("节点数量: " + config.nodeCount);
        System.out.println("测试持续时间: " + config.testDurationSeconds + " 秒");
        System.out.println("基础目录: " + config.baseDir);
        System.out.println("报告目录: " + config.reportDir);
        System.out.println("测试后清理: " + (config.cleanupAfterTest ? "是" : "否"));
        System.out.println("模拟节点故障: " + (config.simulateNodeFailure ? "是" : "否"));
        System.out.println("==============================");
    }
    
    /**
     * 创建测试报告目录
     * 
     * @param config 测试配置
     */
    private static void createReportDirectory(TestConfig config) {
        File reportDir = new File(config.reportDir);
        if (!reportDir.exists()) {
            reportDir.mkdirs();
        }
    }
    
    /**
     * 格式化时间（毫秒转为可读格式）
     * 
     * @param timeMs 时间（毫秒）
     * @return 格式化后的时间字符串
     */
    private static String formatTime(long timeMs) {
        long totalSeconds = timeMs / 1000;
        long minutes = totalSeconds / 60;
        long seconds = totalSeconds % 60;
        long millis = timeMs % 1000;
        
        return String.format("%02d:%02d.%03d", minutes, seconds, millis);
    }
    
    /**
     * 测试配置类
     */
    private static class TestConfig {
        private int nodeCount;
        private int testDurationSeconds;
        
        private String baseDir;
        private String reportDir;
        
        private boolean cleanupAfterTest;
        private boolean simulateNodeFailure;
    }
} 